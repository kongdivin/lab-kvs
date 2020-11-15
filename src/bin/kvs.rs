use kvs::KvStore;
use structopt::StructOpt;

#[derive(StructOpt)]
struct Opt {
	#[structopt(subcommand)]
	cmd: Option<CliCommand>,
}

#[derive(StructOpt)]
enum CliCommand {
	/// Get the string value of a given string key
	Get { key: String },
	/// Set the value of a string key to a string
	Set { key: String, value: String },
	/// Remove a given key
	#[structopt(name = "rm")]
	Remove { key: String },
}

fn main() -> kvs::Result<()> {
	let opt = Opt::from_args();
	let mut kvs = KvStore::open(std::env::current_dir()?).unwrap();

	match opt.cmd {
		Some(CliCommand::Get { key }) => match kvs.get(key) {
			Ok(Some(key)) => {
				println!("{}", key);
				Ok(())
			}
			Ok(None) => {
				println!("Key not found");
				Ok(())
			}
			Err(e) => Err(e),
		},
		Some(CliCommand::Set { key, value }) => kvs.set(key, value),
		Some(CliCommand::Remove { key }) => match kvs.remove(key) {
			Ok(_) => Ok(()),
			Err(e) => {
				println!("Key not found");
				Err(e)
			}
		},
		None => unimplemented!(),
	}
}
