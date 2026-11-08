#![allow(warnings)] //NOTE: TEMPORARY, REMOVE AFTER DEBUG

use std::net::{TcpStream, TcpListener};
use std::io::{self, BufRead, BufReader, Read, Write, Result, Error, ErrorKind};
use std::thread;
use std::sync::mpsc::{self, RecvTimeoutError, Receiver};
use std::collections::{HashSet, HashMap};
use rand::RngExt;
use crate::game::{Game, Player, GamePhase};

mod game;

struct Menu {
    current_game_ids: HashSet<i32>,
    current_games: HashMap<i32, mpsc::Sender<Player>>
}

impl Menu {
    fn initialize_game(&mut self, mut write_stream: TcpStream, rng: &mut impl rand::Rng, rx: &Receiver<String>) -> Result<()> {
        // randomize an id till the id is not in the current_game_ids
        let mut id: i32 = 0;
        while true {
            id = rng.random_range(1000..=9999);
            if !&self.current_game_ids.contains(&id) {
                break;
            }
        }
        self.current_game_ids.insert(id);

        // Get number of players
        let player_num: usize = loop {
            let mut input = String::new();

            Menu::send_message(&mut write_stream, &format!("Please enter the number of player: "));
            
            let input = rx.recv().map_err(|e| {
                Error::new(ErrorKind::Other, format!("Channel receive error: {}", e))
            })?;

            let number : usize = input.trim().parse().unwrap();

            if number < 6 || number > 10 {
                Menu::send_message(&mut write_stream, &format!("The game is only for 6 to 10 players. Please try again!\n"));
                continue;
            } else {
                Menu::send_message(&mut write_stream, &format!("Thanks for your input! This is your game code: { }. Please share this code with other players.\n", id));
                break number;
            }
        };

        // Get the player name
        Menu::send_message(&mut write_stream, &format!("What's your name? "));
        let player_name = rx.recv().map_err(|e| {
            Error::new(ErrorKind::Other, format!("Channel receive error: {}", e))
        })?;

        // Get the signal to start the game
        Menu::send_message(&mut write_stream, &format!("Enter 'GAME' to enter the game."));
        let input = rx.recv().map_err(|e| {
            Error::new(ErrorKind::Other, format!("Channel receive error: {}", e))
        })?;

        // create a chaneel for the menu to talk with the game
        let (join_tx, join_rx) = mpsc::channel();
        self.current_games.insert(id, join_tx);

        // create the player hash map with the creator_stream
        let mut players = HashMap::new();
        let new_player = Player { id: 0, name: player_name, stream: write_stream, faction: None, special_role: None};
        players.insert(0, new_player);

        // initilaize the game with the id and the player
        let mut game = Game {
                        id: id,
                        players: players, 
                        player_count: 1,
                        needed_player_count: player_num,
                        game_phase: GamePhase::Connect, 
                        resist_op_c: player_num - 3, 
                        gov_spy_c: 3, 
                        succeeded_task: 0, 
                        assassin_id: -1, 
                        merlin_id: -1
                    };

        // spawn a thread for the game
        thread::spawn(move || {
            game.start(join_rx)
        });

        Ok(())
    }

    fn join_game(&mut self, mut write_stream: TcpStream, rx: &Receiver<String>) {
        // Get the game id
        'get_game_id: loop {
            Menu::send_message(&mut write_stream, &format!("Please enter game code: "));
            
            let game_id : i32 = rx.recv().map_err(|e| {
                Error::new(ErrorKind::Other, format!("Channel receive error: {}", e))
            }).expect("Unable to parse.").trim().parse().unwrap();

            // find the game id in current games
            for (id, game_tx) in &mut self.current_games {
                if (game_id == *id) {
                    // Get the player name
                    Menu::send_message(&mut write_stream, &format!("What's your name? "));
                    let player_name = rx.recv().map_err(|e| {
                        Error::new(ErrorKind::Other, format!("Channel receive error: {}", e))
                    }).unwrap();

                    // Get the signal for the menu thread to stop listening
                    Menu::send_message(&mut write_stream, &format!("Thank you! Enter 'GAME' to enter the game.\n"));
                    let input = rx.recv().map_err(|e| {
                        Error::new(ErrorKind::Other, format!("Channel receive error: {}", e))
                    }).unwrap();

                    // Create new player and pass it to the channel
                    let new_player = Player { id: -1, name: player_name, stream: write_stream, faction: None, special_role: None};
                    game_tx.send(new_player);
                    break 'get_game_id;
                }
            }

            Menu::send_message(&mut write_stream, &format!("The game code doesnt exists, please try again.\n"));
        }
    }

    fn send_message(stream: &mut TcpStream, message: &str) -> Result<()> {
        stream.write_all(&format!("Menu: ").as_bytes());
        stream.write_all(&message.as_bytes())
    }
}



fn handle_user(stream: TcpStream, tx: mpsc::Sender<String>) {
    // read from player stream
    let reader = BufReader::new(stream);

    // get message and send it to rx
    for line in reader.lines() {
        match line {
            Ok(message) => {
                if message == "GAME" {
                    tx.send(message);
                    println!("Delete the the listening thread!");
                    drop(tx);
                    break;
                } else if tx.send(message).is_err() {
                    break;
                }
                
            }
            Err(e) => {
                println!("User disconnected1");
                break;
            }
        }
    }

}




fn main() -> Result<()> {    
    let tcp_endpoint = "0.0.0.0:5000";
    let listener = TcpListener::bind(tcp_endpoint)?;

    let mut menu = Menu {current_game_ids: HashSet::new(), current_games: HashMap::new()};
    let mut rng = rand::rng();
    // Spawn listener thread
    let (tx, rx) = mpsc::channel::<String>(); // the thread return a message

    // accept all incoming messages
    for stream in listener.incoming() {
        
        match stream {
            Ok(mut writer_stream) => {
                // spawn reading thread
                let tx_clone = tx.clone();
                let mut reader_stream = writer_stream.try_clone()?;
                thread::spawn(move || {
                    handle_user(reader_stream, tx_clone);
                });

                // send message
                Menu::send_message(&mut writer_stream, "Welcome to The Resistance menu. If you want to join a game, enter '1' then hit enter. If you want to start a game, enter '2' then hit enter.\n")?;
                loop {
                    match rx.recv() {
                        Ok(message) => {
                            if message == "1" {
                                Menu::send_message(&mut writer_stream, "You pick 1!\n");
                                menu.join_game(writer_stream, &rx);
                                break;
                            } else if message == "2" {
                                Menu::send_message(&mut writer_stream, "You pick 2!\n");
                                menu.initialize_game(writer_stream, &mut rng, &rx);

                                break;
                            } else {
                                Menu::send_message(&mut writer_stream, "Invalid choice! Please choose again!\n");
                            }
                        }
                        Err(e) => {
                            Menu::send_message(&mut writer_stream, "Error! Please choose again!\n");
                        }
                    }
                }
                
            },
            Err(_) => {}
        }
    }

    println!("End!");

    Ok(())
}
