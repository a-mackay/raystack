// In your own Cargo.toml, add the `tokio` dependency.
// See this crate's Cargo.toml for the versions of these dependencies which
// are currently used in `raystack`.

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    use raystack::eval::eval;
    use raystack::{ClientSeed, ValueExt};

    let timeout_in_seconds = 30;

    // If you are running `eval` many times, reuse the same `ClientSeed`
    // each time you run the `eval` function:
    let client_seed = ClientSeed::new(timeout_in_seconds)?;

    let url = "http://test.com/api/bigProject/";
    let output =
        eval(&client_seed, url, "name", "p4ssw0rd", "readAll(site)", None)
            .await?;
    let sites_grid = output.into_grid();

    // Print the raw JSON:
    println!("{}", sites_grid.to_json_string_pretty());

    // Working with the Grid struct:
    println!("All columns: {:?}", sites_grid.cols());
    println!(
        "first site id: {:?}",
        sites_grid.rows()[0]["id"].as_hs_ref().unwrap()
    );

    Ok(())
}
