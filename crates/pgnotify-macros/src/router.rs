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

fn decode(entry: &Entry) -> TokenStream {
    let channel = &entry.channel;
    let variant = &entry.variant;
    let value = match (&entry.payload, &entry.field) {
        (Some(ty), Some(field)) => {
            quote! { Self::#variant { #field: <#ty as ::pgnotify::hook::NotifyPayload>::decode_payload(notify.payload_bytes())? } }
        }
        (Some(ty), None) => {
            quote! { Self::#variant(<#ty as ::pgnotify::hook::NotifyPayload>::decode_payload(notify.payload_bytes())?) }
        }
        (None, _) => quote! { {
            if !notify.payload.is_empty() {
                return Err(::pgnotify::error::PgNotifyError::CodecError { reason: "unit notification payload must be empty" });
            }
            Self::#variant
        } },
    };
    quote! {
        if notify.channel != #channel { return Ok(None); }
        if notify.payload.len() > ::pgnotify::types::PayloadBuf::CAPACITY {
            return Err(::pgnotify::error::PgNotifyError::PayloadTooLarge {
                len: notify.payload.len(), max: ::pgnotify::types::PayloadBuf::CAPACITY,
            });
        }
        Ok(Some(#value))
    }
}

fn collision_tree(entries: &[&Entry]) -> syn::Result<TokenStream> {
    let Some(first) = entries.first() else {
        return Ok(quote!(Ok(None)));
    };
    if entries.len() == 1 {
        return Ok(decode(first));
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
            let subtree = collision_tree(&group)?;
            Ok(quote!(#byte => { #subtree }))
        })
        .collect::<syn::Result<Vec<_>>>()?;
    Ok(quote! { match __pgnotify_bytes[#offset] { #(#arms,)* _ => Ok(None) } })
}

fn emit(input: &DeriveInput, entries: &[Entry]) -> syn::Result<TokenStream> {
    let name = &input.ident;
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
            .push(parse_quote!(#ty: ::pgnotify::hook::NotifyPayload));
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
            (Some(ty), Some(field)) => quote!(Self::#variant { #field: __pgnotify_value } => <#ty as ::pgnotify::hook::NotifyPayload>::encode_payload(__pgnotify_value, out)),
            (Some(ty), None) => quote!(Self::#variant(__pgnotify_value) => <#ty as ::pgnotify::hook::NotifyPayload>::encode_payload(__pgnotify_value, out)),
            (None, _) => quote!(Self::#variant => { out.clear(); Ok(()) }),
        }
    });
    let mut groups: BTreeMap<(usize, u64), Vec<&Entry>> = BTreeMap::new();
    for entry in entries {
        groups
            .entry((entry.bytes.len(), prefix(&entry.bytes)))
            .or_default()
            .push(entry);
    }
    let decode_arms = groups
        .into_iter()
        .map(|((len, prefix), entries)| {
            let tree = collision_tree(&entries)?;
            Ok(quote!((#len, #prefix) => { #tree }))
        })
        .collect::<syn::Result<Vec<_>>>()?;
    Ok(quote! {
        impl #impl_generics ::pgnotify::hook::NotifyRouter for #name #ty_generics #where_clause {
            const CHANNELS: &'static [&'static str] = &[#(#channels),*];
            const LISTEN_BATCH_SQL: &'static str = #sql;
            fn channel_name(&self) -> &'static str { match self { #(#name_arms,)* } }
            fn encode_event_payload(&self, out: &mut ::pgnotify::types::PayloadBuf) -> ::pgnotify::error::Result<()> {
                match self { #(#encode_arms,)* }
            }
            fn decode_notification(notify: ::pgnotify::notify_ref::NotificationRef<'_>) -> ::pgnotify::error::Result<Option<Self>> {
                let __pgnotify_bytes = notify.channel.as_bytes();
                let mut __pgnotify_prefix = [0u8; 8];
                let __pgnotify_count = __pgnotify_bytes.len().min(8);
                __pgnotify_prefix[..__pgnotify_count].copy_from_slice(&__pgnotify_bytes[..__pgnotify_count]);
                match (__pgnotify_bytes.len(), u64::from_le_bytes(__pgnotify_prefix)) {
                    #(#decode_arms,)* _ => Ok(None),
                }
            }
        }
        impl #impl_generics #name #ty_generics #where_clause {
            /// Adapt typed notifications and errors to a single callback.
            #visibility fn router_hook<#callback_type>(callback: #callback_type) -> #hook<Self, #callback_type>
            where #callback_type: FnMut(::pgnotify::error::Result<Self>) {
                #hook::new(callback)
            }
        }
        /// Routes known notifications and forwards codec and listener errors.
        #visibility struct #hook<R, F> {
            callback: F,
            router: ::core::marker::PhantomData<fn() -> R>,
        }
        impl<R, F> #hook<R, F> where R: ::pgnotify::hook::NotifyRouter, F: FnMut(::pgnotify::error::Result<R>) {
            /// Construct a notification and error callback adapter.
            #visibility fn new(callback: F) -> Self { Self { callback, router: ::core::marker::PhantomData } }
            /// Recover the callback and its captured state.
            #visibility fn into_inner(self) -> F { self.callback }
        }
        impl<R, F> ::pgnotify::hook::NotifyHook for #hook<R, F>
        where R: ::pgnotify::hook::NotifyRouter, F: FnMut(::pgnotify::error::Result<R>) {
            fn on_notify(&mut self, notify: ::pgnotify::notify_ref::NotificationRef<'_>) {
                match R::decode_notification(notify) {
                    Ok(Some(event)) => (self.callback)(Ok(event)),
                    Ok(None) => {},
                    Err(error) => ::pgnotify::hook::NotifyHook::on_error(self, error),
                }
            }
            fn on_error(&mut self, error: ::pgnotify::error::PgNotifyError) { (self.callback)(Err(error)); }
        }
    })
}
