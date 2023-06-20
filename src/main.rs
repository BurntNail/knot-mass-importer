use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};
use csv_async::AsyncReaderBuilder;
use futures::stream::StreamExt;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use tokio::fs::read_to_string;

pub struct PersonPlusEvents {
    pub first_name: String,
    pub surname: String,
    pub form: String,
    pub event_db_ids: Vec<i32>,
}

const YEAR_SEPARATOR: &str = "YEAR SEPARATOR";

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq)]
pub enum Year {
    _4,
    L5,
    U5,
    L6,
    U6,
}

impl Year {
    pub const fn event_prefix(self) -> &'static str {
        use Year::*;
        match self {
            _4 => "4th",
            L5 => "L5",
            U5 => "U5",
            L6 => "L6",
            U6 => "U6",
        }
    }

    pub const fn get_year(event_year: Self, person_year: Self) -> usize {
        use Year::*;
        let join_year = match person_year {
            _4 => 2022,
            L5 => 2021,
            U5 => 2020,
            L6 => 2019,
            U6 => 2018,
        };
        let offset = match event_year {
            _4 => 0,
            L5 => 1,
            U5 => 2,
            L6 => 3,
            U6 => 4,
        };
        join_year + offset
    }
}

impl From<String> for Year {
    fn from(st: String) -> Year {
        use Year::*;
        match st.as_str() {
            "4K" => _4,
            "L5K" => L5,
            "U5K" => U5,
            "L6K" => L6,
            "U6K" => U6,
            x => panic!("Invalid year: {x:?}"),
        }
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct EventInfo {
    pub name: String,
    pub event_year: Year,
}

impl std::fmt::Display for EventInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.event_year.event_prefix(), self.name)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv()?;

    let pool = PgPoolOptions::new()
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;
    println!("All connected!");

    let file_to_read = std::env::args().nth(1).expect("missing csv file to read");
    println!("Reading {file_to_read:?}");
    let file_contents = read_to_string(file_to_read).await?;
    let rdr = AsyncReaderBuilder::new()
        .has_headers(false)
        .create_reader(file_contents.as_bytes());
    let mut rdr = rdr.into_records();

    //(EventInfo, Year) represents (info, current year of people in this event)
    let mut event_db_ids: HashMap<(EventInfo, Year), usize> = HashMap::new();
    let mut events: HashMap<usize, EventInfo> = HashMap::new(); //hm not a vec as blank cols for eventinfos

    let mut i = 0;
    while let Some(record) = rdr.next().await.transpose()? {
        if i == 0 {
            let mut years = vec![Year::_4, Year::L5, Year::U5, Year::L6, Year::U6];
            let mut current = years.remove(0);

            let mut col_index = 0;
            for event_name in record.iter().skip(2).map(|x| x.trim().to_string()) {
                if event_name == YEAR_SEPARATOR {
                    current = years.remove(0);
                } else {
                    events.insert(
                        col_index,
                        EventInfo {
                            name: event_name,
                            event_year: current,
                        },
                    );
                }
                col_index += 1;
            }
        } else {
            let full_name = record[0].to_string();
            let raw_form = record[1].to_string();
            let form: Year = raw_form.clone().into();

            print!("{}: {full_name} = ", form.event_prefix());
            let participant_id = sqlx::query!(
                "SELECT id FROM people WHERE CONCAT(first_name, ' ', surname) = $1 and form = $2",
                full_name,
                raw_form
            )
            .fetch_one(&pool)
            .await?
            .id;
            print!("{participant_id}");

			let mut count = 0;
            for event_col_index in record
                .iter()
                .skip(2)
                .enumerate()
                .filter_map(|(i, contents)| if contents.is_empty() { None } else { Some(i) })
            {
                let event_info = events[&event_col_index].clone();

				let key = (event_info.clone(), form);
                let event_db_id = match event_db_ids.get(&key) {
                    Some(id) => *id,
                    None => {
                        let id = sqlx::query!(
                            r#"
    			INSERT INTO events (event_name, date, location, teacher) 
    			VALUES ($1, $2, $3, $4) RETURNING id"#,
                            event_info.to_string(),
                            NaiveDateTime::new(
                                NaiveDate::from_ymd_opt(
                                    Year::get_year(event_info.event_year, form) as i32,
                                    9,
                                    1
                                )
                                .unwrap(),
                                NaiveTime::from_hms_opt(9, 0, 0).unwrap()
                            ),
                            "KCS",
                            "MJPC"
                        )
                        .fetch_one(&pool)
                        .await?
                        .id as usize;
                        event_db_ids.insert(key, id);
                        id
                    }
                };

                sqlx::query!(
                    r#"
    			    INSERT INTO public.participant_events
    			    (participant_id, event_id)
    			    VALUES($1, $2);            
    			                "#,
                    participant_id as i32,
                    event_db_id as i32
                )
                .execute(&pool)
                .await?;

                count += 1;
            }

            println!(" with {count} events.");
        }

        i += 1;
    }

    Ok(())
}
