use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

use petgraph::graph::NodeIndex;

pub trait DbKey: 'static {
    type Value: 'static;
}

pub trait DataBase {
    fn get<K: DbKey>(&self) -> Option<&K::Value>;
    fn get_cloned<K: DbKey>(&self) -> Option<K::Value>
    where
        K::Value: Clone,
    {
        self.get::<K>().cloned()
    }
    fn put<K: DbKey>(&mut self, value: K::Value) -> Option<K::Value>;
}

pub struct InMemoryDb {
    data: HashMap<TypeId, Box<dyn Any>>,
}

impl InMemoryDb {
    pub fn new() -> Self {
        InMemoryDb {
            data: HashMap::new(),
        }
    }
}

impl DataBase for InMemoryDb {
    fn get<K: DbKey>(&self) -> Option<&K::Value> {
        let t = TypeId::of::<K>();
        self.data.get(&t).and_then(|v| v.downcast_ref::<K::Value>())
    }

    fn put<K: DbKey>(&mut self, value: K::Value) -> Option<K::Value> {
        self.data
            .insert(TypeId::of::<K>(), Box::new(value))
            .and_then(|v| v.downcast::<K::Value>().ok().map(|v| *v))
    }
}

pub trait Task<Db: DataBase> {
    type Input: TaskInput<Db>;
    type Output: TaskOutput<Db>;

    fn execute(input: Self::Input) -> Self::Output;
}

impl DbKey for () {
    type Value = ();
}

impl<Db: DataBase> TaskInput<Db> for () {
    fn from_db(_db: &Db) -> Self {
        ()
    }
}

impl<Db: DataBase> TaskOutput<Db> for () {
    fn to_db(&self, _db: &mut Db) {}
}

pub trait TaskInput<Db: DataBase>: DbKey<Value = Self>
where
    Self: Sized + 'static,
{
    fn from_db(db: &Db) -> Self;
    fn dep_types() -> Vec<TypeId> {
        vec![]
    }
}

pub trait TaskOutput<Db: DataBase>: DbKey<Value = Self>
where
    Self: Sized + 'static,
{
    fn to_db(&self, db: &mut Db);
    fn out_types() -> Vec<TypeId> {
        vec![]
    }
}

pub struct ExecutionGraph<Db: DataBase> {
    tasks: petgraph::graph::DiGraph<TypeId, fn(&mut Db)>,
    db: Db,
}

impl<Db: DataBase> ExecutionGraph<Db> {
    pub fn new(db: Db) -> Self {
        ExecutionGraph {
            db,
            tasks: petgraph::graph::DiGraph::new(),
        }
    }

    fn contains_node(&self, ty: &TypeId) -> Option<NodeIndex> {
        self.tasks.node_indices().find(|i| &self.tasks[*i] == ty)
    }

    pub fn execute<T: Task<Db>>(&mut self) -> T::Output {
        for ty in T::Input::dep_types() {
            if let None = self.contains_node(&ty) {
                panic!("Missing dependency: {:?}", ty)
            }
        }
        let input = T::Input::from_db(&self.db);
        let output = T::execute(input);
        output.to_db(&mut self.db);
        output
    }
}

pub struct ExecutionGraphBuilder<Db: DataBase> {
    graph: ExecutionGraph<Db>,
}

impl<Db: DataBase> ExecutionGraphBuilder<Db> {
    pub fn new(db: Db) -> Self {
        ExecutionGraphBuilder {
            graph: ExecutionGraph::new(db),
        }
    }

    pub fn add_input<T: DbKey>(&mut self, value: T::Value) -> &mut Self {
        self.graph.db.put::<T>(value);
        self
    }

    pub fn add_task<T: Task<Db>>(&mut self) -> &mut Self {
        let task_input_node = self.graph.tasks.add_node(TypeId::of::<T::Input>());
        for dep_ty in T::Input::dep_types() {
            let Some(in_node_id) = self.graph.contains_node(&dep_ty) else {
                panic!("Missing dependency: {:?}", dep_ty)
            };

            self.graph
                .tasks
                .add_edge(in_node_id, task_input_node, |db| {
                    let input = T::Input::from_db(db);
                    db.put::<T::Input>(input);
                });
        }
        let out_node = self.graph.tasks.add_node(TypeId::of::<T::Output>());
        for out_ty in T::Output::out_types() {
            match self.graph.contains_node(&out_ty) {
                Some(_out_node_id) => {
                    panic!("Output already exists: {:?}", out_ty)
                }
                None => {
                    let out_ty_node = self.graph.tasks.add_node(out_ty);
                    self.graph.tasks.add_edge(out_node, out_ty_node, |_| {});
                }
            }
        }
        self
    }

    pub fn build(self) -> ExecutionGraph<Db> {
        self.graph
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MyKey;

    impl DbKey for MyKey {
        type Value = i32;
    }

    #[test]
    fn test_in_memory_db() {
        let mut db = InMemoryDb::new();
        db.put::<MyKey>(42);
        assert_eq!(db.get::<MyKey>(), Some(&42));
    }

    #[test]
    fn test_in_memory_db_wrong_key() {
        let mut db = InMemoryDb::new();
        db.put::<MyKey>(42);
        assert_eq!(db.get::<MyKey>(), Some(&42));
        assert_eq!(db.get::<MyKey>(), Some(&42));
    }

    #[derive(Copy, Clone)]
    struct MyValue {
        x: i32,
    }

    impl DbKey for MyValue {
        type Value = MyValue;
    }

    impl<Db: DataBase> TaskInput<Db> for MyValue {
        fn from_db(db: &Db) -> Self {
            db.get_cloned::<MyValue>().unwrap()
        }
    }

    #[derive(Copy, Clone, PartialEq, Debug)]
    struct MyValue2 {
        x: i32,
    }

    impl DbKey for MyValue2 {
        type Value = MyValue2;
    }

    impl<Db: DataBase> TaskOutput<Db> for MyValue2 {
        fn to_db(&self, db: &mut Db) {
            db.put::<MyValue2>(*self);
        }
    }

    struct MyTask;

    impl Task<InMemoryDb> for MyTask {
        type Input = MyValue;
        type Output = MyValue2;

        fn execute(input: Self::Input) -> Self::Output {
            MyValue2 { x: input.x }
        }
    }

    #[test]
    fn test_execution_graph() {
        let mut builder = ExecutionGraphBuilder::new(InMemoryDb::new());
        builder.add_input::<MyValue>(MyValue { x: 42 });
        builder.add_task::<MyTask>();
        let mut graph = builder.build();
        graph.execute::<MyTask>();
        assert_eq!(graph.db.get::<MyValue2>(), Some(&MyValue2 { x: 42 }));
    }
}
