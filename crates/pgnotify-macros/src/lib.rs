//! Derives for typed PostgreSQL notification payloads and channel routing.
//!
//! Payload types also implement `sonic_rs::Serialize` and owned `Deserialize`.
//! Encoding writes to the caller's bounded `PayloadBuf`; no intermediate JSON
//! string is created. Router variants use `#[channel = "name"]` and contain
//! either no fields or one payload field. SQL identifiers are always quoted.
//!
//! Each enum emits `<Enum>RouterHook<R, F>` and `Enum::router_hook(F)` in its
//! module. The callback has type `FnMut(pgnotify::Result<R>)`: known events
//! arrive as `Ok`, codec/listener errors as `Err`, and unknown channels are
//! ignored. Distinct enum names can therefore share the same module.

#![forbid(unsafe_code)]

mod router;

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input, parse_quote};

fn runtime_path() -> proc_macro2::TokenStream {
    match proc_macro_crate::crate_name("pgnotify") {
        Ok(proc_macro_crate::FoundCrate::Itself) => quote!(crate),
        Ok(proc_macro_crate::FoundCrate::Name(name)) => {
            let name = syn::Ident::new(&name, proc_macro2::Span::call_site());
            quote!(::#name)
        }
        Err(_) => quote!(::pgnotify),
    }
}

/// Implement the notification payload codec for a serializable owned type.
#[proc_macro_derive(NotifyPayload)]
pub fn derive_notify_payload(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let runtime = runtime_path();
    let mut generics = input.generics.clone();
    let mut lifetime_name = String::from("__pgnotify_de");
    while generics
        .lifetimes()
        .any(|parameter| parameter.lifetime.ident == lifetime_name)
    {
        lifetime_name.push('_');
    }
    let lifetime = syn::Lifetime::new(
        &format!("'{lifetime_name}"),
        proc_macro2::Span::mixed_site(),
    );
    generics.make_where_clause().predicates.push(parse_quote!(
        Self: #runtime::sonic_api::Serialize + for<#lifetime> #runtime::sonic_api::Deserialize<#lifetime>
    ));
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    quote! {
        impl #impl_generics #runtime::hook::NotifyPayload for #name #ty_generics #where_clause {
            #[inline]
            fn encode_payload(&self, out: &mut #runtime::types::PayloadBuf)
                -> #runtime::error::Result<()>
            {
                out.clear();
                if #runtime::sonic_api::to_writer(&mut *out, self).is_err() {
                    #runtime::__private::cold_path();
                    let error = if out.overflowed() {
                        #runtime::error::PgNotifyError::PayloadTooLarge {
                            len: #runtime::types::PayloadBuf::CAPACITY + 1,
                            max: #runtime::types::PayloadBuf::CAPACITY,
                        }
                    } else {
                        #runtime::error::PgNotifyError::CodecError {
                            reason: "JSON serialization failed",
                        }
                    };
                    out.clear();
                    return ::core::result::Result::Err(error);
                }
                ::core::result::Result::Ok(())
            }

            #[inline]
            fn decode_payload(bytes: &[u8]) -> #runtime::error::Result<Self> {
                if bytes.len() > #runtime::types::PayloadBuf::CAPACITY {
                    return ::core::result::Result::Err(
                        #runtime::error::PgNotifyError::PayloadTooLarge {
                            len: bytes.len(), max: #runtime::types::PayloadBuf::CAPACITY,
                        }
                    );
                }
                #runtime::sonic_api::from_slice(bytes).map_err(|_| {
                    #runtime::__private::cold_path();
                    #runtime::error::PgNotifyError::CodecError { reason: "JSON deserialization failed" }
                })
            }
        }
    }.into()
}

/// Implement typed channel routing for an annotated enum.
///
/// Channel names contain 1..=63 UTF-8 bytes without NUL and must be unique.
/// Dispatch matches length and the first eight bytes, then resolves prefix
/// collisions with a bounded byte discriminator tree and an exact name check.
#[proc_macro_derive(NotifyRouter, attributes(channel))]
pub fn derive_notify_router(input: TokenStream) -> TokenStream {
    match router::expand(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}
