//! Channel validation and bounded discriminator generation.

use std::collections::{BTreeMap, BTreeSet};

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Expr, Fields, Ident, Lit, LitStr, Meta, Type, parse_quote};

struct Entry {
    variant: Ident,
    channel: LitStr,
    bytes: Vec<u8>,
    field: Option<Ident>,
    payload: Option<Type>,
}

pub(crate) fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input: DeriveInput = syn::parse2(input)?;
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "NotifyRouter requires an enum",
        ));
    };
    if data.variants.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "NotifyRouter requires at least one channel",
        ));
    }
    let mut channels = BTreeSet::new();
    let mut entries = Vec::new();
    for variant in &data.variants {
        let mut channel = None;
        for attr in variant
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("channel"))
        {
            if channel.is_some() {
                return Err(syn::Error::new_spanned(attr, "duplicate channel attribute"));
            }
            if let Meta::NameValue(meta) = &attr.meta
                && let Expr::Lit(value) = &meta.value
                && let Lit::Str(value) = &value.lit
            {
                channel = Some(value.clone());
            } else {
                return Err(syn::Error::new_spanned(
                    attr,
                    "expected #[channel = \"name\"]",
                ));
            }
        }
        let channel = channel
            .ok_or_else(|| syn::Error::new_spanned(variant, "missing #[channel = \"name\"]"))?;
        let name = channel.value();
        if name.is_empty() || name.len() > 63 || name.as_bytes().contains(&0) {
            return Err(syn::Error::new_spanned(
                channel,
                "channel must contain 1..=63 UTF-8 bytes without NUL",
            ));
        }
        if !channels.insert(name.clone()) {
            return Err(syn::Error::new_spanned(channel, "duplicate channel name"));
        }
        let (field, payload) = match &variant.fields {
            Fields::Unit => (None, None),
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                (None, fields.unnamed.first().map(|field| field.ty.clone()))
            }
            Fields::Named(fields) if fields.named.len() == 1 => {
                let field = fields.named.first();
                (
                    field.and_then(|field| field.ident.clone()),
                    field.map(|field| field.ty.clone()),
                )
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    &variant.fields,
                    "expected a unit or single-field variant",
                ));
            }
        };
        entries.push(Entry {
            variant: variant.ident.clone(),
            channel,
            bytes: name.into_bytes(),
            field,
            payload,
        });
    }
    emit(&input, &entries)
}

fn prefix(bytes: &[u8]) -> u64 {
    let mut prefix = [0; 8];
    for (to, from) in prefix.iter_mut().zip(bytes) {
        *to = *from;
    }
    u64::from_le_bytes(prefix)
}

fn decode(entry: &Entry, runtime: &TokenStream) -> TokenStream {
    let channel = &entry.channel;
    let variant = &entry.variant;
    let value = match (&entry.payload, &entry.field) {
        (Some(ty), Some(field)) => {
            quote! { Self::#variant { #field: <#ty as #runtime::hook::NotifyPayload>::decode_payload(notify.payload_bytes())? } }
        }
        (::core::option::Option::Some(ty), ::core::option::Option::None) => {
            quote! { Self::#variant(<#ty as #runtime::hook::NotifyPayload>::decode_payload(notify.payload_bytes())?) }
        }
        (::core::option::Option::None, _) => quote! { {
            if !notify.payload.is_empty() {
                return ::core::result::Result::Err(#runtime::error::PgNotifyError::CodecError { reason: "unit notification payload must be empty" });
            }
            Self::#variant
        } },
    };
    quote! {
        if notify.channel != #channel { #runtime::__private::cold_path(); return ::core::result::Result::Ok(::core::option::Option::None); }
        if notify.payload.len() > #runtime::types::PayloadBuf::CAPACITY {
            return ::core::result::Result::Err(#runtime::error::PgNotifyError::PayloadTooLarge {
                len: notify.payload.len(), max: #runtime::types::PayloadBuf::CAPACITY,
            });
        }
        ::core::result::Result::Ok(::core::option::Option::Some(#value))
    }
}

fn collision_tree(entries: &[&Entry], runtime: &TokenStream) -> syn::Result<TokenStream> {
    let Some(first) = entries.first() else {
        return Ok(quote!({
            #runtime::__private::cold_path();
            ::core::result::Result::Ok(::core::option::Option::None)
        }));
    };
    if entries.len() == 1 {
        return Ok(decode(first, runtime));
    }
    // Each level examines a distinct byte, bounding collision work by 63 bytes.
    let offset = (8..first.bytes.len())
        .find(|&offset| {
            entries
                .iter()
                .any(|entry| entry.bytes[offset] != first.bytes[offset])
        })
        .ok_or_else(|| {
            syn::Error::new_spanned(&first.channel, "duplicate channel discriminator")
        })?;
    let mut groups: BTreeMap<u8, Vec<&Entry>> = BTreeMap::new();
    for entry in entries {
        groups.entry(entry.bytes[offset]).or_default().push(entry);
    }
    let arms = groups
        .into_iter()
        .map(|(byte, group)| {
            let subtree = collision_tree(&group, runtime)?;
            Ok(quote!(#byte => { #subtree }))
        })
        .collect::<syn::Result<Vec<_>>>()?;
    Ok(
        quote! { match __pgnotify_bytes[#offset] { #(#arms,)* _ => { #runtime::__private::cold_path(); ::core::result::Result::Ok(::core::option::Option::None) } } },
    )
}

fn emit(input: &DeriveInput, entries: &[Entry]) -> syn::Result<TokenStream> {
    let name = &input.ident;
    let runtime = crate::runtime_path();
    let visibility = &input.vis;
    let hook = format_ident!("{}RouterHook", name);
    let mut callback_type = format_ident!("__PgNotifyCallback");
    while input
        .generics
        .params
        .iter()
        .any(|parameter| match parameter {
            syn::GenericParam::Type(parameter) => parameter.ident == callback_type,
            syn::GenericParam::Const(parameter) => parameter.ident == callback_type,
            syn::GenericParam::Lifetime(_) => false,
        })
    {
        callback_type = format_ident!("{}_", callback_type);
    }
    let mut generics = input.generics.clone();
    for ty in entries.iter().filter_map(|entry| entry.payload.as_ref()) {
        generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#ty: #runtime::hook::NotifyPayload));
    }
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let channels = entries.iter().map(|entry| &entry.channel);
    let sql = entries
        .iter()
        .map(|entry| format!("LISTEN \"{}\";", entry.channel.value().replace('"', "\"\"")))
        .collect::<String>();
    let name_arms = entries.iter().map(|entry| {
        let variant = &entry.variant;
        let channel = &entry.channel;
        let pattern = match (&entry.payload, &entry.field) {
            (Some(_), Some(_)) => quote!(Self::#variant { .. }),
            (Some(_), None) => quote!(Self::#variant(..)),
            (None, _) => quote!(Self::#variant),
        };
        quote!(#pattern => #channel)
    });
    let encode_arms = entries.iter().map(|entry| {
        let variant = &entry.variant;
        match (&entry.payload, &entry.field) {
            (Some(ty), Some(field)) => quote!(Self::#variant { #field: __pgnotify_value } => <#ty as #runtime::hook::NotifyPayload>::encode_payload(__pgnotify_value, out)),
            (Some(ty), None) => quote!(Self::#variant(__pgnotify_value) => <#ty as #runtime::hook::NotifyPayload>::encode_payload(__pgnotify_value, out)),
            (None, _) => quote!(Self::#variant => { out.clear(); ::core::result::Result::Ok(()) }),
        }
    });
    let mut groups: BTreeMap<usize, BTreeMap<u64, Vec<&Entry>>> = BTreeMap::new();
    for entry in entries {
        groups
            .entry(entry.bytes.len())
            .or_default()
            .entry(prefix(&entry.bytes))
            .or_default()
            .push(entry);
    }
    let decode_arms = groups
        .into_iter()
        .map(|(len, groups)| {
            let bytes = (0..8).map(|offset| {
                if offset < len {
                    quote!(__pgnotify_bytes[#offset])
                } else {
                    quote!(0u8)
                }
            });
            let arms = groups
                .into_iter()
                .map(|(prefix, entries)| {
                    let tree = collision_tree(&entries, &runtime)?;
                    Ok(quote!(#prefix => { #tree }))
                })
                .collect::<syn::Result<Vec<_>>>()?;
            Ok(quote!(#len => {
                let __pgnotify_prefix = ::core::primitive::u64::from_le_bytes([#(#bytes),*]);
                match __pgnotify_prefix { #(#arms,)* _ => { #runtime::__private::cold_path(); ::core::result::Result::Ok(::core::option::Option::None) } }
            }))
        })
        .collect::<syn::Result<Vec<_>>>()?;
    ::core::result::Result::Ok(quote! {
        impl #impl_generics #runtime::hook::NotifyRouter for #name #ty_generics #where_clause {
            const CHANNELS: &'static [&'static ::core::primitive::str] = &[#(#channels),*];
            const LISTEN_BATCH_SQL: &'static ::core::primitive::str = #sql;
            #[inline]
            fn channel_name(&self) -> &'static ::core::primitive::str { match self { #(#name_arms,)* } }
            #[inline]
            fn encode_event_payload(&self, out: &mut #runtime::types::PayloadBuf) -> #runtime::error::Result<()> {
                match self { #(#encode_arms,)* }
            }
            #[inline]
            fn decode_notification(notify: #runtime::notify_ref::NotificationRef<'_>) -> #runtime::error::Result<::core::option::Option<Self>> {
                let __pgnotify_bytes = notify.channel.as_bytes();
                match __pgnotify_bytes.len() {
                    #(#decode_arms,)* _ => { #runtime::__private::cold_path(); ::core::result::Result::Ok(::core::option::Option::None) },
                }
            }
        }
        impl #impl_generics #name #ty_generics #where_clause {
            /// Adapt typed notifications and errors to a single callback.
            #visibility fn router_hook<#callback_type>(callback: #callback_type) -> #hook<Self, #callback_type>
            where #callback_type: ::core::ops::FnMut(#runtime::error::Result<Self>) {
                #hook::new(callback)
            }
        }
        /// Routes known notifications and forwards codec and listener errors.
        #visibility struct #hook<R, F> {
            callback: F,
            router: ::core::marker::PhantomData<fn() -> R>,
        }
        impl<R, F> #hook<R, F> where R: #runtime::hook::NotifyRouter, F: ::core::ops::FnMut(#runtime::error::Result<R>) {
            /// Construct a notification and error callback adapter.
            #visibility fn new(callback: F) -> Self { Self { callback, router: ::core::marker::PhantomData } }
            /// Recover the callback and its captured state.
            #visibility fn into_inner(self) -> F { self.callback }
        }
        impl<R, F> #runtime::hook::NotifyHook for #hook<R, F>
        where R: #runtime::hook::NotifyRouter, F: ::core::ops::FnMut(#runtime::error::Result<R>) {
            #[inline]
            fn on_notify(&mut self, notify: #runtime::notify_ref::NotificationRef<'_>) {
                match R::decode_notification(notify) {
                    ::core::result::Result::Ok(::core::option::Option::Some(event)) => (self.callback)(::core::result::Result::Ok(event)),
                    ::core::result::Result::Ok(::core::option::Option::None) => { #runtime::__private::cold_path(); },
                    ::core::result::Result::Err(error) => #runtime::hook::NotifyHook::on_error(self, error),
                }
            }
            #[inline]
            fn on_error(&mut self, error: #runtime::error::PgNotifyError) { (self.callback)(::core::result::Result::Err(error)); }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dispatch_generation_uses_hints_through_the_runtime_reexport() -> syn::Result<()> {
        let tokens = expand(quote!(
            enum Route {
                #[channel = "orders"]
                Orders,
            }
        ))?;
        let parsed: syn::File = syn::parse2(tokens.clone())?;
        let mut inlined = 0;
        for item in parsed.items {
            if let syn::Item::Impl(implementation) = item {
                for item in implementation.items {
                    if let syn::ImplItem::Fn(method) = item
                        && [
                            "channel_name",
                            "encode_event_payload",
                            "decode_notification",
                            "on_notify",
                            "on_error",
                        ]
                        .contains(&method.sig.ident.to_string().as_str())
                    {
                        assert!(method.attrs.iter().any(|attr| {
                            attr.path()
                                .segments
                                .last()
                                .is_some_and(|segment| segment.ident == "inline")
                        }));
                        inlined += 1;
                    }
                }
            }
        }
        assert_eq!(inlined, 5);
        let generated = tokens.to_string();
        assert!(generated.contains("cold_path"));
        assert!(!generated.contains(":: uarch"));
        Ok(())
    }
}
