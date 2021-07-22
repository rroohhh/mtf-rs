use failure::Error;
use futures_lite::stream::StreamExt;
use oxidized_mdf::pages::Record;
use oxidized_mdf::MdfDatabase;
use pretty_hex::{config_hex, HexConfig};
use prettytable::{Cell, Row, Table};
use std::convert::TryFrom;
use std::path::PathBuf;

use byteorder::{LittleEndian, ReadBytesExt};
use mdf::{BootPage, PagePointer, PageProvider};
use mtf::mdf::MTFPageProvider;
use mtf::{MTFParser, StreamWithData};

fn main() -> Result<(), Error> {
    let file = &std::env::args().collect::<Vec<_>>()[1];
    let mut f = MTFParser::new(&file);
    let mut db_stream = None;
    for dblk in f.dblks() {
        // println!("dblk: {:#?}", dblk.dblk);
        for stream in dblk.streams {
            if stream.stream.header.id == "MQDA" {
                db_stream = Some(stream);
            }
        }
    }

    let stream = db_stream.unwrap();

    let page_provider = MTFPageProvider::from_stream(stream);
    let boot_page = BootPage::parse(page_provider.get(PagePointer {
        file_id: 1,
        page_id: 9,
    }));
    println!("{:#?}", boot_page);
    let first_sys_indices = page_provider.get(boot_page.first_sys_indices);
    println!("{:#?}", first_sys_indices.header);
    for record in first_sys_indices.records() {
        println!("{:#?}", record);
    }
    /*
    println!("{:#?}", boot_page.header);
    let cfg = HexConfig { width: 32, group: 0, title: false, ..HexConfig::default() };
    println!("{}", config_hex(&boot_page.data, cfg));
    */
    Ok(())
}

/*
#[async_std::main]
async fn main() -> Result<(), Error> {
    let mut f = MTFParser::new("");
    // let mut db_stream = None;
    for dblk in f.dblks() {
        println!("dblk: {:#?}", dblk.dblk);
        for stream in dblk.streams {
            // println!("stream: {:#?}", stream.stream);
//            if stream.stream.header.id == "MQDA" {
//                println!("found mqda stream");
                let cfg = HexConfig { width: 32, group: 0, title: false, ..HexConfig::default() };
                let len = stream.data.len();
                println!("{:#?}", stream.stream);

                let mut pos = 2;
                while (pos + 8192) < len {
                    let mut buffer = [0u8; 8192];
                    buffer.copy_from_slice(&stream.data[pos..pos + 8192]);
                    let page = oxidized_mdf::pages::Page::try_from(buffer).unwrap();
                    //if page.header.ty == 13 {
//                        println!("[{:<08}] {}", pos / 8192, config_hex(&(&stream.data[pos..pos + 8192]), cfg));
                    //}
                    println!("[{:<08}] {:?}", pos / 8192, page.header);

                    /*
                    if page.header.ty != 8 && page.header.ty != 9 && page.header.ty != 16 && page.header.ty != 17 && page.header.ty != 10 && page.header.ty != 2 && page.header.ty != 3 {
                        let records = page.records();

                        for (i, record) in records.into_iter().enumerate() {
                            let Record { r#type, variable_columns, .. } = record;
                            println!("[{:<04}] {:#?}", i, r#type);
                            if variable_columns.is_some() {
                                for (j, col) in variable_columns.unwrap().enumerate() {
                                    println!("[{:<04}][{:<04}] {:#?}", i, j, col);
                                }
                            }
                        }
                    }
                    */

                    /*
                    let hdr = oxidized_mdf::pages::PageHeader::try_from(&stream.data[pos..pos + 96]).unwrap();
                    println!("[{:<08}] {:?}", pos / 8192, hdr);
                    println!("{}", config_hex(&(&stream.data[pos..pos + 96 + 32]), cfg));
                    */

                    pos += 8192;
                }
            /*
                let len= stream.data.len();
                println!("{}", config_hex(&(&stream.data[0..8192.min(len)]), cfg));

             */
//                db_stream = Some(stream);
//            }
        }
    }

    /*
    let stream = db_stream.unwrap();
    let cursor = async_std::io::Cursor::new(&stream.data[2..]);
    let db = MdfDatabase::from_read(Box::new(cursor));
    dump(db).await;
    */

    Ok(())
}

async fn print_rows(db: &mut MdfDatabase<'_>, table: &str, row_limit: &Option<usize>) {
}

async fn dump(db: impl std::future::Future<Output = std::result::Result<MdfDatabase<'_>, oxidized_mdf::error::Error>>) {
    let mut db = db.await.unwrap();
    let row_limit = &Some(1000);
    for table in db.table_names() {
        {
        let mut rows = match MdfDatabase::rows(&mut db, &table) {
            Some(rows) => rows,
            None => {
                eprintln!("No table {}", table);
                return;
            }
        };

        let mut pretty_table = Table::new();

        let mut i = 0usize;
        while let Some(row) = rows.next().await {
            let values = row.values();

            if pretty_table.is_empty() {
                let cells = values.iter().map(|(k, _)| Cell::new(k)).collect::<Vec<_>>();
                pretty_table.add_row(Row::new(cells));
            }

            let cells = values
                .into_iter()
                .map(|(_, v)| Cell::new(&format!("{}", v)))
                .collect::<Vec<_>>();
            pretty_table.add_row(Row::new(cells));

            i += 1;

            if matches!(row_limit, Some(row_limit) if i >= *row_limit) {
                break;
            }
        }

        println!("--------------------");
        println!("Data of table: {}", table);
        println!("--------------------");
        pretty_table.printstd();
        }
    }
}
 */
