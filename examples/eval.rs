// In your own Cargo.toml, add the dependencies `tokio` and `url`.
// See this crate's Cargo.toml for the versions of these dependencies which
// are currently used in `raystack`.

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    use raystack::{ClientSeed, SkySparkClient, ValueExt};
    use url::Url;

    let url = Url::parse("https://www.example.com/api/projName/")?;
    let timeout_in_seconds = 30;

    // If you are creating many `SkySparkClient`s, reuse the same `ClientSeed`
    // for each `SkySparkClient`:
    let client_seed = ClientSeed::new(timeout_in_seconds)?;

    let mut client =
        SkySparkClient::new(url, "username", "p4ssw0rd", client_seed).await?;

    let sites_grid = client.eval("readAll(site)").await?;

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
