use wasm_bindgen::prelude::*;
use wasm_bindgen_test::*;

use aqora_client::{Client as BaseClient, GraphQLQuery};

use aqora_data_utils::wasm::io::set_console_error_panic_hook;

const USERNAME: &str = "alice";
const PASSWORD: &str = "P@ssw0rD";
const GRAPHQL_HOST: &str = "http://localhost:8080";

fn graphql_url() -> String {
    format!("{GRAPHQL_HOST}/graphql")
}

type Client = BaseClient<String>;

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "tests/web/mod.graphql",
    schema_path = "../schema.graphql"
)]
struct LoginUserMutation;

async fn login_user() -> String {
    let body = LoginUserMutation::build_query(login_user_mutation::Variables {
        username_or_email: USERNAME.to_string(),
        password: PASSWORD.to_string(),
    });
    let response = reqwest::Client::new()
        .post(graphql_url())
        .json(&body)
        .send()
        .await
        .unwrap();
    if let Some(access_token) = response.headers().get("X-Access-Token") {
        access_token.to_str().unwrap().to_string()
    } else {
        panic!("Bad login: {}", response.text().await.unwrap());
    }
}

async fn authenticated_client() -> Client {
    let access_token = login_user().await;
    aqora_client::Client::new(graphql_url().parse().unwrap(), access_token)
}

#[derive(GraphQLQuery)]
#[graphql(
    query_path = "tests/web/mod.graphql",
    schema_path = "../schema.graphql"
)]
struct CreateDatasetMutation;

fn rand_slug() -> String {
    let crypto = js_sys::global()
        .dyn_into::<web_sys::WorkerGlobalScope>()
        .unwrap()
        .crypto()
        .unwrap();
    format!("test-{}", crypto.random_uuid().split('-').next().unwrap())
}

async fn create_dataset(
    client: &Client,
) -> create_dataset_mutation::CreateDatasetMutationCreateDatasetNode {
    client
        .send::<CreateDatasetMutation>(create_dataset_mutation::Variables { slug: rand_slug() })
        .await
        .unwrap()
        .create_dataset
        .node
}

#[wasm_bindgen_test]
async fn test_login_user() {
    set_console_error_panic_hook();
    let client = authenticated_client().await;
    let dataset = create_dataset(&client).await;
    console_log!("{} {}", dataset.id, dataset.slug);
}
