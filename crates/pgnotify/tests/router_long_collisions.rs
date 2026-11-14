use pgnotify::{NotificationRef, hook::NotifyRouter as _};
use pgnotify_macros::NotifyRouter;
#[derive(Debug, PartialEq, NotifyRouter)]
enum Routes {
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx!tailtail"]
    V0,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"tailtail"]
    V1,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx#tailtail"]
    V2,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx$tailtail"]
    V3,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx%tailtail"]
    V4,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx&tailtail"]
    V5,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'tailtail"]
    V6,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx(tailtail"]
    V7,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx)tailtail"]
    V8,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx*tailtail"]
    V9,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx+tailtail"]
    V10,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx,tailtail"]
    V11,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-tailtail"]
    V12,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.tailtail"]
    V13,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/tailtail"]
    V14,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx0tailtail"]
    V15,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx1tailtail"]
    V16,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx2tailtail"]
    V17,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx3tailtail"]
    V18,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx4tailtail"]
    V19,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx5tailtail"]
    V20,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx6tailtail"]
    V21,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx7tailtail"]
    V22,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx8tailtail"]
    V23,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx9tailtail"]
    V24,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:tailtail"]
    V25,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx;tailtail"]
    V26,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx<tailtail"]
    V27,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx=tailtail"]
    V28,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx>tailtail"]
    V29,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx?tailtail"]
    V30,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx@tailtail"]
    V31,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxAtailtail"]
    V32,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxBtailtail"]
    V33,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxCtailtail"]
    V34,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxDtailtail"]
    V35,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxEtailtail"]
    V36,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxFtailtail"]
    V37,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxGtailtail"]
    V38,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxHtailtail"]
    V39,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxItailtail"]
    V40,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxJtailtail"]
    V41,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxKtailtail"]
    V42,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxLtailtail"]
    V43,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxMtailtail"]
    V44,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxNtailtail"]
    V45,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxOtailtail"]
    V46,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxPtailtail"]
    V47,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxQtailtail"]
    V48,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxRtailtail"]
    V49,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxStailtail"]
    V50,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxTtailtail"]
    V51,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxUtailtail"]
    V52,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxVtailtail"]
    V53,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxWtailtail"]
    V54,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxXtailtail"]
    V55,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxYtailtail"]
    V56,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxZtailtail"]
    V57,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx[tailtail"]
    V58,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\\tailtail"]
    V59,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx]tailtail"]
    V60,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx^tailtail"]
    V61,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_tailtail"]
    V62,
    #[channel = "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`tailtail"]
    V63,
}
#[test]
fn all_long_router_collisions_are_exact() {
    for (channel, expected) in [
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx!tailtail",
            Routes::V0,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"tailtail",
            Routes::V1,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx#tailtail",
            Routes::V2,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx$tailtail",
            Routes::V3,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx%tailtail",
            Routes::V4,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx&tailtail",
            Routes::V5,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'tailtail",
            Routes::V6,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx(tailtail",
            Routes::V7,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx)tailtail",
            Routes::V8,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx*tailtail",
            Routes::V9,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx+tailtail",
            Routes::V10,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx,tailtail",
            Routes::V11,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-tailtail",
            Routes::V12,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.tailtail",
            Routes::V13,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/tailtail",
            Routes::V14,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx0tailtail",
            Routes::V15,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx1tailtail",
            Routes::V16,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx2tailtail",
            Routes::V17,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx3tailtail",
            Routes::V18,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx4tailtail",
            Routes::V19,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx5tailtail",
            Routes::V20,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx6tailtail",
            Routes::V21,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx7tailtail",
            Routes::V22,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx8tailtail",
            Routes::V23,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx9tailtail",
            Routes::V24,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:tailtail",
            Routes::V25,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx;tailtail",
            Routes::V26,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx<tailtail",
            Routes::V27,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx=tailtail",
            Routes::V28,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx>tailtail",
            Routes::V29,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx?tailtail",
            Routes::V30,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx@tailtail",
            Routes::V31,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxAtailtail",
            Routes::V32,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxBtailtail",
            Routes::V33,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxCtailtail",
            Routes::V34,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxDtailtail",
            Routes::V35,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxEtailtail",
            Routes::V36,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxFtailtail",
            Routes::V37,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxGtailtail",
            Routes::V38,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxHtailtail",
            Routes::V39,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxItailtail",
            Routes::V40,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxJtailtail",
            Routes::V41,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxKtailtail",
            Routes::V42,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxLtailtail",
            Routes::V43,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxMtailtail",
            Routes::V44,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxNtailtail",
            Routes::V45,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxOtailtail",
            Routes::V46,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxPtailtail",
            Routes::V47,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxQtailtail",
            Routes::V48,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxRtailtail",
            Routes::V49,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxStailtail",
            Routes::V50,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxTtailtail",
            Routes::V51,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxUtailtail",
            Routes::V52,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxVtailtail",
            Routes::V53,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxWtailtail",
            Routes::V54,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxXtailtail",
            Routes::V55,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxYtailtail",
            Routes::V56,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxZtailtail",
            Routes::V57,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx[tailtail",
            Routes::V58,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\\tailtail",
            Routes::V59,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx]tailtail",
            Routes::V60,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx^tailtail",
            Routes::V61,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_tailtail",
            Routes::V62,
        ),
        (
            "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`tailtail",
            Routes::V63,
        ),
    ] {
        assert_eq!(
            Routes::decode_notification(NotificationRef {
                backend_pid: 0,
                channel_id: None,
                channel,
                payload: ""
            }),
            Ok(Some(expected))
        );
    }
    assert_eq!(
        Routes::decode_notification(NotificationRef {
            backend_pid: 0,
            channel_id: None,
            channel: "abcdefghxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxztailtail",
            payload: ""
        }),
        Ok(None)
    );
}
