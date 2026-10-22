use crate::application::commands::AppServices;
use crate::application::commands::tests::test_services;
use crate::application::ports::{CreateAccountInput, CreateNoteInput, JobKind};
use crate::federation::outbox::FederateDeliverPayload;

fn remote_account(services: &AppServices, handle: &str) -> String {
    services
        .accounts
        .create_account(
            CreateAccountInput {
                handle: handle.to_string(),
                host: Some("remote.example".to_string()),
                display_name: None,
                description: None,
                location: None,
                birthday: None,
                uri: Some(format!("https://remote.example/users/{handle}")),
                inbox: Some("https://remote.example/inbox".to_string()),
                shared_inbox: None,
                is_bot: false,
                is_cat: false,
                is_locked: false,
                preserved_usernames: Vec::new(),
            },
            None,
        )
        .unwrap()
        .id
}

fn federate_jobs(services: &AppServices) -> Vec<FederateDeliverPayload> {
    let mut out = Vec::new();
    while let Some(job) = services.jobs.claim_next(60_000).unwrap() {
        if job.kind == JobKind::FederateDeliver {
            out.push(serde_json::from_slice(&job.payload).unwrap());
        }
        services.jobs.complete(&job.id).unwrap();
    }
    out
}

#[test]
fn undo_like_names_the_like_that_was_delivered() {
    // Regression (bench run 2, S2 residue): `unreact` built the Undo(Like)
    // from the reaction *row* id, but `react` had federated the Like under
    // a different id. Akkoma/Pleroma resolves an Undo's object by id and
    // silently drops it when unknown, so the remote favourite survived.
    let services = test_services("notes-undo-like");
    let alice = services.create_account("alice", None).unwrap().id;
    let rina = remote_account(&services, "rina");
    let note = services
        .handle_inbound_create_note(
            "https://remote.example/activities/1",
            CreateNoteInput {
                uri: Some("https://remote.example/notes/1".to_string()),
                ..CreateNoteInput::simple_public(&rina, "hello")
            },
            Vec::new(),
        )
        .unwrap()
        .expect("remote note stored");

    services.react(&alice, &note.id, "🎉", false).unwrap();
    let like_id = match federate_jobs(&services).as_slice() {
        [FederateDeliverPayload::Like { reaction_id, .. }] => reaction_id.clone(),
        other => panic!("expected one Like, got {other:?}"),
    };

    services.unreact(&alice, &note.id).unwrap();
    match federate_jobs(&services).as_slice() {
        [FederateDeliverPayload::UndoLike { reaction_id, .. }] => {
            assert_eq!(
                reaction_id, &like_id,
                "Undo(Like) must reference the Like id that was delivered"
            );
        }
        other => panic!("expected one UndoLike, got {other:?}"),
    }
}
