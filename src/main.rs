#[derive(Debug)]
struct Task {
    inner: String,
}

impl Task {
    fn new(s: &str) -> Self {
        Task {
            inner: s.to_string(),
        }
    }
}

struct MultiQueue<T> {
    vec: Vec<T>,
}

impl<T> MultiQueue<T> {
    fn new(vec: Vec<T>) -> Self {
        MultiQueue { vec }
    }

    fn get<P>(&mut self, pred: P) -> Option<T>
    where
        P: FnMut(&T) -> bool,
    {
        let idx = self.vec.iter().position(pred)?;
        let elt = self.vec.swap_remove(idx);
        Some(elt)
    }
}

fn main() {
    let a = Task::new("a");
    let b = Task::new("b");
    let c = Task::new("c");

    let mut q = MultiQueue::new(vec![a, b, c]);

    let task = q.get(|t| t.inner == "a");
    println!("Got task! {:?}", task);
    let task = q.get(|t| t.inner == "a");
    println!("Got task! {:?}", task);
}
