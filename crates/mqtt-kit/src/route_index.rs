//! 以 arena 儲存 topic trie，插入、查詢與回收都不依賴遞迴呼叫。
use mqtt_kit_core::TopicFilter;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
struct Node {
    children: HashMap<String, usize>,
    exact: HashSet<TopicFilter>,
    descendants: HashSet<TopicFilter>,
}

pub(crate) struct RouteIndex {
    nodes: Vec<Node>,
    free: Vec<usize>,
}
impl Default for RouteIndex {
    fn default() -> Self {
        Self {
            nodes: vec![Node::default()],
            free: Vec::new(),
        }
    }
}
impl RouteIndex {
    pub(crate) fn insert(&mut self, filter: &TopicFilter) {
        let mut node = 0;
        for level in filter.as_str().split('/') {
            if level == "#" {
                self.nodes[node].descendants.insert(filter.clone());
                return;
            }
            node = if let Some(child) = self.nodes[node].children.get(level) {
                *child
            } else {
                let child = self.free.pop().unwrap_or_else(|| {
                    self.nodes.push(Node::default());
                    self.nodes.len() - 1
                });
                self.nodes[node].children.insert(level.to_owned(), child);
                child
            };
        }
        self.nodes[node].exact.insert(filter.clone());
    }
    pub(crate) fn remove(&mut self, filter: &TopicFilter) {
        let mut node = 0;
        let mut path = Vec::new();
        let mut hash = false;
        for level in filter.as_str().split('/') {
            if level == "#" {
                self.nodes[node].descendants.remove(filter);
                hash = true;
                break;
            }
            let Some(child) = self.nodes[node].children.get(level).copied() else {
                return;
            };
            path.push((node, level.to_owned(), child));
            node = child;
        }
        if !hash {
            self.nodes[node].exact.remove(filter);
        }
        for (parent, level, child) in path.into_iter().rev() {
            let node = &self.nodes[child];
            if !node.children.is_empty() || !node.exact.is_empty() || !node.descendants.is_empty() {
                break;
            }
            self.nodes[parent].children.remove(&level);
            self.nodes[child] = Node::default();
            self.free.push(child);
        }
    }
    pub(crate) fn matches(&self, topic: &str) -> Vec<TopicFilter> {
        if topic.len() > usize::from(u16::MAX)
            || topic.is_empty()
            || topic.as_bytes().contains(&0)
            || topic.contains(['+', '#'])
        {
            return Vec::new();
        }
        let levels: Vec<_> = topic.split('/').collect();
        let mut frontier = vec![(0, 0)];
        let mut matches = Vec::new();
        while let Some((node_id, depth)) = frontier.pop() {
            let node = &self.nodes[node_id];
            let system_root = depth == 0 && topic.starts_with('$');
            if !system_root {
                matches.extend(node.descendants.iter().cloned());
            }
            if depth == levels.len() {
                matches.extend(node.exact.iter().cloned());
                continue;
            }
            if let Some(child) = node.children.get(levels[depth]) {
                frontier.push((*child, depth + 1));
            }
            if !system_root {
                if let Some(child) = node.children.get("+") {
                    frontier.push((*child, depth + 1));
                }
            }
        }
        matches
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn indexed_results_match_reference_semantics_and_removals() {
        let filters = [
            "#",
            "+",
            "sport/#",
            "sport/+",
            "sport/+/score",
            "/+",
            "a//b",
            "$SYS/#",
            "+/status",
            "a/+/b/#",
        ]
        .map(|s| TopicFilter::new(s).unwrap());
        let mut index = RouteIndex::default();
        for filter in &filters {
            index.insert(filter);
        }
        for topic in [
            "sport",
            "sport/",
            "sport/x/score",
            "/",
            "a//b",
            "$SYS/status",
            "$OTHER/status",
            "a/x/b/y",
            "",
            "a/+",
            "a\0b",
        ] {
            let expected: HashSet<_> = filters
                .iter()
                .filter(|f| f.matches(topic))
                .cloned()
                .collect();
            assert_eq!(
                index.matches(topic).into_iter().collect::<HashSet<_>>(),
                expected,
                "{topic:?}"
            );
        }
        for filter in &filters {
            index.remove(filter);
        }
        assert!(index.matches("sport/x/score").is_empty());
        assert_eq!(index.free.len(), index.nodes.len() - 1);
        index.insert(&filters[2]);
        assert_eq!(index.matches("sport/x"), vec![filters[2].clone()]);
    }
    #[test]
    fn unrelated_topics_do_not_visit_each_filter() {
        let mut index = RouteIndex::default();
        for n in 0..10000 {
            index.insert(&TopicFilter::new(format!("device/{n}/status")).unwrap());
        }
        assert_eq!(index.matches("device/9999/status").len(), 1);
        assert!(index.matches("unrelated/topic").is_empty());
    }
}
