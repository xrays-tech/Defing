//! P2b3: openraft 0.10 membership-change retry wrapper (join / promote / remove).
//!
//! openraft rejects a membership proposal while the PREVIOUS membership log entry
//! has not committed yet:
//!   RaftError::APIError(ClientWriteError::ChangeMembershipError(InProgress{..}))
//! That rejection is transient - the earlier change commits shortly, and the retry
//! then passes openraft's lease/commit checks. The three production membership
//! paths absorb it with a bounded retry budget instead of surfacing a 500 (UI/CLI
//! otherwise sees a spurious join/promote/remove failure whenever two admin
//! operations race). ForwardToLeader, EmptyMembership, LearnerNotFound, Fatal and
//! an exhausted retry budget fall through to the handler's existing mapping.

use dsh_raft::openraft::error::ChangeMembershipError;
use dsh_raft::openraft::error::ClientWriteError;
use dsh_raft::openraft::error::RaftError;
use dsh_raft::openraft::raft::ClientWriteResponse;
use dsh_raft::NodeInfo as RaftNodeInfo;
use dsh_raft::RaftHandle;
use dsh_raft::TypeConfig;

type MembershipError = RaftError<TypeConfig, ClientWriteError<TypeConfig>>;

const RETRY_LIMIT: u8 = 8;
const RETRY_DELAY_MS: u64 = 250;

/// True only for `ClientWriteError::ChangeMembershipError(::InProgress(..))`:
/// the previous membership entry is not committed yet (transient by design).
pub fn is_membership_in_progress(e: &MembershipError) -> bool {
    match e.api_error() {
        Some(ClientWriteError::ChangeMembershipError(ChangeMembershipError::InProgress(_))) => true,
        _other => false,
    }
}

/// The three membership request shapes the handlers issue.
pub enum MembershipOp {
    AddLearner {
        id: u64,
        node: RaftNodeInfo,
    },
    ReplaceVoters {
        voters: Vec<u64>,
    },
    Change {
        members: dsh_raft::openraft::ChangeMembers<u64, RaftNodeInfo>,
    },
}

/// Run a membership write, absorbing transient InProgress rejections (<=8 attempts,
/// 250ms apart); anything else returns to the caller for its normal mapping.
pub async fn membership_write(
    raft: &RaftHandle,
    op: MembershipOp,
) -> Result<ClientWriteResponse<TypeConfig>, MembershipError> {
    let mut tries: u8 = 0;
    loop {
        tries += 1;
        let attempt = match op {
            MembershipOp::AddLearner { id, ref node } => {
                raft.add_learner(id, node.clone(), false).await
            }
            MembershipOp::ReplaceVoters { ref voters } => {
                raft.change_membership(voters.clone(), false).await
            }
            MembershipOp::Change { ref members } => {
                raft.change_membership(members.clone(), false).await
            }
        };
        match attempt {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                if tries < RETRY_LIMIT && is_membership_in_progress(&e) {
                    // previous membership entry commits within ms; bounded nap then re-propose
                    tokio::time::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS)).await;
                    continue;
                }
                return Err(e);
            }
        };
    }
}
