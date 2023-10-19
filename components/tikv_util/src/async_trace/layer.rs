use std::{
    borrow::Cow,
    cell::RefCell,
    io::Cursor,
    sync::{Arc, Mutex},
};

use dashmap::{mapref::entry::Entry, DashMap};
use tracing::{span, subscriber, Subscriber};
use tracing_subscriber::{prelude::*, registry::LookupSpan, Layer};

use super::{data::Data, tree::Tree};
use crate::async_trace::tree::FormatTreeTo;

lazy_static::lazy_static! {
    static ref GLOBAL_LAYER: CurrentStacksLayer = CurrentStacksLayer::default();
}

/// Initial the `tracing` subscriber
pub fn init() {
    subscriber::set_global_default(tracing_subscriber::registry().with(GLOBAL_LAYER.clone()))
        .expect("failed to set the global dispatcher for async_trace, if you are adding `tracing` support for TiKV, please read the comment.");
}

pub fn dump_all_tree_bytes() -> Vec<u8> {
    GLOBAL_LAYER.fmt_bytes()
}

pub fn get_span_trees() -> &'static DashMap<span::Id, Arc<Mutex<Tree>>> {
    &GLOBAL_LAYER.roots
}

#[derive(Default, Clone)]
pub struct CurrentStacksLayer {
    roots: Arc<DashMap<span::Id, Arc<Mutex<Tree>>>>,
}

impl CurrentStacksLayer {
    pub fn get_span_trees(&self) -> &DashMap<span::Id, Arc<Mutex<Tree>>> {
        &self.roots
    }

    pub fn fmt_string(&self) -> String {
        let res = self.fmt_bytes();
        match String::from_utf8_lossy(&res) {
            // SAFETY: `from_utf8_lossy` returns the origin string, it must be a valid string.
            Cow::Borrowed(_) => unsafe { String::from_utf8_unchecked(res) },
            Cow::Owned(rep) => rep,
        }
    }

    fn fmt_bytes(&self) -> Vec<u8> {
        use std::io::Write;
        let mut res = Cursor::new(vec![]);
        for ent in self.roots.iter() {
            let id = ent.key();
            let tree = ent.value();
            writeln!(res, "{}", id.into_u64()).unwrap();
            tree.lock()
                .unwrap()
                .traverse_with(FormatTreeTo::new(&mut res))
                .unwrap();
        }
        res.into_inner()
    }
}

impl<S> Layer<S> for CurrentStacksLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx.span(id);
        if let Some(span) = span {
            if span.extensions().get::<Data>().is_none() {
                span.extensions_mut().insert(Data::from_attribute(attrs))
            }
        }
    }

    fn on_enter(&self, id: &span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        CURRENT_FRAME_STACK.with(|cell| {
            let mut rt = cell.borrow_mut();
            match &*rt {
                None => {
                    let ptr = match self.roots.entry(id.clone()) {
                        Entry::Occupied(o) => o.get().clone(),
                        Entry::Vacant(v) => {
                            let ptr = Arc::new(Mutex::new(Tree::new_with_root(id.clone())));
                            debug!("inserting new root span."; "category" => "async_trace::layer", "id" => ?id);
                            v.insert(ptr.clone());
                            ptr
                        }
                    };
                    debug_assert!(ptr.lock().unwrap().on_root());
                    *rt = Some(ptr);
                    return;
                }
                Some(rt) => {
                    let mut tree = rt.lock().unwrap();
                    tree.step_in(id);
                }
            }
        })
    }

    fn on_exit(&self, id: &span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        CURRENT_FRAME_STACK.with(|cell| {
            let mut rt = cell.borrow_mut();
            if let Some(tree) = &*rt {
                let mut t = tree.lock().unwrap();
                debug_assert!(t.current_span() == id);
                if t.on_root() {
                    drop(t);
                    *rt = None;
                } else {
                    t.step_out()
                }
            }
        })
    }

    fn on_close(&self, id: span::Id, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        debug!("span closed."; "category" => "async_trace::layer", "id" => ?id);
        CURRENT_FRAME_STACK.with(|cell| {
            let c = cell.borrow();
            if let Some(c) = c.as_ref() {
                let mut tree = c.lock().unwrap();
                let removed = tree.remove_child(&id);
                debug_assert!(
                    removed,
                    "current node isn't the parent of the dropped span {id:?}; tree = {tree:?}"
                );
            }
        });
        // In this case, the remained part of tree must be dropped.
        // There is a corner case that when you are dropping a batch of spans, the
        // `on_close` may be triggered unordered. (That may happen when you are
        // using `tracing_futures::Instrument`, which won't enter the span while
        // dropping.), in that case we may fail to drop a subtree.
        debug_assert!(
            self.roots
                .get(&id)
                .map(|tree| tree.lock().unwrap().only_root())
                .unwrap_or(true),
            "subtree leaking\n{}",
            self.roots.get(&id).unwrap().lock().unwrap().fmt_string()
        );
        // If it is a root, remove the whole tree.
        self.roots.remove(&id);
    }
}

thread_local! {
    static CURRENT_FRAME_STACK: RefCell<Option<Arc<Mutex<Tree>>>> = RefCell::new(None);
}
