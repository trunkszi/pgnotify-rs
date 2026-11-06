//! One stored control word; public generations retain their independent full u64 range.
use super::ListenerPhase;
use crate::error::ListenerState;
use uarch::prelude::*;

bitword! {
    pub(super) struct FsmStateWord(u64) {
        [0..=7] pub(super) PHASE: u8,
        [8] pub(super) READ_INTEREST,
        [9] pub(super) WRITE_INTEREST,
        [10] pub(super) TIMER_ARMED,
        [11] pub(super) RECONNECTING,
        [12] pub(super) DRAINING,
        [13] pub(super) HAS_PENDING_CMDS,
        [32..=63] pub(super) RETRIES: u32,
    }
}
uarch::layout::assert_layout!(FsmStateWord, size = 8, align = 8);
impl FsmStateWord {
    #[inline]
    pub(super) fn disconnected() -> Self {
        Self::new(0).with_phase(ListenerPhase::Disconnected)
    }
    #[hot_inline]
    pub(super) const fn phase(self) -> ListenerPhase {
        // SAFETY: disconnected and with_phase write PHASE exclusively from valid enum
        // values; all other updates touch disjoint proven fields or flags.
        unsafe { core::mem::transmute::<u8, ListenerPhase>(self.get(Self::PHASE)) }
    }
    #[inline]
    pub(super) fn with_phase(self, phase: ListenerPhase) -> Self {
        let state = phase.state();
        let reading = matches!(
            state,
            ListenerState::ConnectingReading
                | ListenerState::ResettingReading
                | ListenerState::FlushingSubscriptions
                | ListenerState::IdlePingFlushing
                | ListenerState::AwaitingCommandAck
                | ListenerState::Listening
                | ListenerState::IdlePingAwaiting
        );
        let writing = matches!(
            state,
            ListenerState::ConnectingWriting
                | ListenerState::ResettingWriting
                | ListenerState::FlushingSubscriptions
                | ListenerState::IdlePingFlushing
        );
        let reconnecting = matches!(
            state,
            ListenerState::ResettingReading | ListenerState::ResettingWriting
        );
        let pending = matches!(
            state,
            ListenerState::FlushingSubscriptions
                | ListenerState::AwaitingCommandAck
                | ListenerState::IdlePingFlushing
                | ListenerState::IdlePingAwaiting
        );
        self.patch(
            uarch::bits::BitPatch::EMPTY
                .with_field(Self::PHASE, phase as u64)
                .with_flag(Self::READ_INTEREST, reading)
                .with_flag(Self::WRITE_INTEREST, writing)
                .with_flag(Self::RECONNECTING, reconnecting)
                .with_flag(Self::DRAINING, phase.is_terminating())
                .with_flag(Self::HAS_PENDING_CMDS, pending),
        )
    }
    #[hot_inline]
    pub(super) fn next_retry(self) -> Self {
        // 96 doublings already saturate Duration; saturation cannot shorten a public generation.
        self.wrapping_add_top_field(
            Self::RETRIES,
            predicate(self.get(Self::RETRIES) < 96) * 1_u64,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stored_phase_projection_preserves_independent_flags_and_retry_counter() {
        let phases = [
            ListenerPhase::Disconnected,
            ListenerPhase::BackoffWait,
            ListenerPhase::ConnectingReading,
            ListenerPhase::ConnectingWriting,
            ListenerPhase::ResettingReading,
            ListenerPhase::ResettingWriting,
            ListenerPhase::FlushingSubscriptions,
            ListenerPhase::AwaitingCommandAck,
            ListenerPhase::Listening,
            ListenerPhase::IdlePingFlushing,
            ListenerPhase::IdlePingAwaiting,
            ListenerPhase::FlushingReplay,
            ListenerPhase::AwaitingReplayAck,
            ListenerPhase::FlushingSubscriptionsContinuation,
            ListenerPhase::AwaitingCommandAckContinuation,
            ListenerPhase::ListeningContinuation,
            ListenerPhase::IdlePingFlushingContinuation,
            ListenerPhase::IdlePingAwaitingContinuation,
            ListenerPhase::FlushingReplayContinuation,
            ListenerPhase::AwaitingReplayAckContinuation,
            ListenerPhase::ShuttingDown,
            ListenerPhase::Terminated,
        ];
        let mut word =
            FsmStateWord::disconnected().patch(patch!(FsmStateWord { TIMER_ARMED: true }));
        for _ in 0..100 {
            word = word.next_retry();
        }
        assert_eq!(word.get(FsmStateWord::RETRIES), 96);
        for phase in phases {
            word = word.with_phase(phase);
            assert_eq!(word.phase(), phase);
            assert!(word.test(FsmStateWord::TIMER_ARMED));
            assert_eq!(word.get(FsmStateWord::RETRIES), 96);
            assert_eq!(word.test(FsmStateWord::DRAINING), phase.is_terminating());
        }
        let reading = word.with_phase(ListenerPhase::ConnectingReading);
        assert!(reading.matches(pattern!(FsmStateWord { READ_INTEREST & !WRITE_INTEREST })));
        let pending = word.with_phase(ListenerPhase::FlushingReplay);
        assert!(
            pending.matches(
                pattern!(FsmStateWord { READ_INTEREST & WRITE_INTEREST & HAS_PENDING_CMDS })
            )
        );
        let reset = word.with_phase(ListenerPhase::ResettingWriting);
        assert!(
            reset
                .matches(pattern!(FsmStateWord { RECONNECTING & WRITE_INTEREST & !READ_INTEREST }))
        );
    }
}
