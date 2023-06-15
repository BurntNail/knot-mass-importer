use tokio::fs::read_to_string;
use sqlx::postgres::PgPoolOptions;
use csv_async::AsyncReaderBuilder;
use futures::stream::StreamExt;
use chrono::naive::{NaiveDateTime, NaiveDate, NaiveTime};

pub struct PersonPlusEvents {
	pub first_name: String,
	pub surname: String,
	pub form: String,
	pub event_db_ids: Vec<i32>
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv()?;

    let pool = PgPoolOptions::new().connect(&std::env::var("DATABASE_URL")?).await?;

    println!("All connected!");

    let file_to_read = std::env::args().nth(1).expect("missing csv file to read");
    println!("Reading {file_to_read:?}");

    let file_contents = read_to_string(file_to_read).await?;

    let rdr = AsyncReaderBuilder::new().has_headers(false).create_reader(file_contents.as_bytes());
    let mut rdr = rdr.into_records();

    let mut events = vec![];
    let mut combined = vec![];


	let mut i = 0;
    while let Some(record) = rdr.next().await.transpose()? {
    	if i == 0 {
    		events = record.iter().skip(2).map(|x| x.to_string()).collect::<Vec<_>>();
    	} else if i == 1{
    		let mut years = record.iter().skip(2).map(|x| x.parse());
    		for name in &events {
    			let id = sqlx::query!(
    			            r#"
    			INSERT INTO events (event_name, date, location, teacher) 
    			VALUES ($1, $2, $3, $4) RETURNING id"#,
    			            name,
    			            NaiveDateTime::new(NaiveDate::from_ymd_opt(years.next().expect("expected_year")?, 9, 1).unwrap(), NaiveTime::from_hms_opt(09, 00, 00).unwrap()),
    			            "KCS",
    			            "MJPC"
    			     
    			        )
    			        .fetch_one(&pool)
    			        .await?;
    		
    			combined.push(id.id);
    		}
    	} else {
    		let first_name = record[0].to_string();
    		let surname = record[1].to_string();
    		let form = record[2].to_string();
    		let events = record.iter().skip(3).flat_map(|x| x.parse::<usize>().ok());

    		let Ok(participant_id) = sqlx::query!("SELECT id FROM people WHERE first_name = $1 and surname = $2 and form = $3", first_name, surname, form).fetch_one(&pool).await else {
				let events = events.collect::<Vec<_>>();
    			eprintln!("Unable to do {first_name} {surname} in {form} with {events:?}");
    			continue;
    		};
    		let participant_id = participant_id.id;
    		println!("Doing {participant_id}");

    		let mut count = 0;
    		for event_db_id in events.map(|i| combined[i]) {
    			sqlx::query!(
    			                r#"
    			    INSERT INTO public.participant_events
    			    (participant_id, event_id)
    			    VALUES($1, $2);            
    			                "#,
    			                participant_id,
    			                event_db_id
    			            )
    			            .execute(&pool)
    			            .await?;

    			count += 1;
    		}

    		println!("Added {first_name} {surname} to {count} events.");
    	}
    	
    	i += 1;
    }

    println!("{combined:?}");

    Ok(())
}
