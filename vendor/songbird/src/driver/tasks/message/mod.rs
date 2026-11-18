#![allow(missing_docs)]

mod core;
mod disposal;
mod events;
mod mixer;
#[cfg(feature = "receive")]
mod udp_rx;
mod ws;

#[cfg(feature = "receive")]
pub use self::udp_rx::*;
pub use self::{core::*, disposal::*, events::*, mixer::*, ws::*};

use crate::driver::dave_stats::DaveTxStats;
use flume::Sender;
use std::sync::Arc;
use tokio::spawn;
use tracing::trace;

#[derive(Clone, Debug)]
pub struct Interconnect {
    pub core: Sender<CoreMessage>,
    pub events: Sender<EventMessage>,
    pub mixer: Sender<MixerMessage>,
    pub(crate) dave_tx_stats: Arc<DaveTxStats>,
}

impl Interconnect {
    pub fn poison(&self) {
        drop(self.events.send(EventMessage::Poison));
    }

    pub fn poison_all(&self) {
        drop(self.mixer.send(MixerMessage::Poison));
        self.poison();
    }

    pub fn restart_volatile_internals(&mut self) {
        self.poison();

        let (evt_tx, evt_rx) = flume::unbounded();

        self.events = evt_tx;

        spawn(async move {
            trace!("Event processor restarted.");
            super::events::runner(evt_rx).await;
            trace!("Event processor finished.");
        });

        // Make mixer aware of new targets...
        drop(
            self.mixer
                .send(MixerMessage::ReplaceInterconnect(self.clone())),
        );
    }
}
