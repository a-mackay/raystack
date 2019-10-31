// In your own Cargo.toml, add the dependencies `ring`, `tokio` and `url`.
// See this crate's Cargo.toml for the versions of these dependencies which
// are currently used in `raystack`.

#[tokio::main]
async fn main() {
    use raystack::{SkySparkClient, ValueExt};
    use ring::rand::SystemRandom;
    use url::Url;

    let rng = SystemRandom::new();
    let url = Url::parse("https://www.example.com/api/projName/").unwrap();
    let client = SkySparkClient::new(url, "username", "p4ssw0rd", None, &rng)
        .await
        .unwrap();

    let sites_grid = client.eval("readAll(site)").await.unwrap();

    // Print the raw JSON:
    println!("{}", sites_grid.to_json_string_pretty());

    // Working with the Grid struct:
    println!("All columns: {:?}", sites_grid.cols());
    println!(
        "first site id: {:?}",
        sites_grid.rows()[0]["id"].as_hs_ref().unwrap()
    );
}
