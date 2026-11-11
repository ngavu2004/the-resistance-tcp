#![allow(warnings)] //NOTE: TEMPORARY, REMOVE AFTER DEBUG

use std::net::{TcpStream, TcpListener};
use std::io::{self, BufRead, BufReader, Read, Write, Result, Error, ErrorKind};
use std::thread;
use std::sync::mpsc::{self, RecvTimeoutError, Receiver};
use std::collections::{HashSet, HashMap};
use rand::{RngExt, rngs::ThreadRng};
use crate::game::{Game, Player, GamePhase};
use std::sync::{Arc, Mutex};

mod game;

struct Menu {
    current_game_ids: HashSet<i32>,
    current_games: HashMap<i32, mpsc::Sender<Player>>,
}

struct PlayerRequest{
    player: Player,
    create_game: bool,
    player_num: Option<usize>,
    game_id: Option<i32>
}

impl Menu {
    fn initialize_game(&mut self, mut player: Player, player_num: usize) -> Result<()> {
        // randomize an id till the id is not in the current_game_ids
        let mut rng = rand::rng();
        let mut id: i32 = 0;
        while true {
            id = rng.random_range(1000..=9999);
            if !&self.current_game_ids.contains(&id) {
                break;
            }
        }
        self.current_game_ids.insert(id);
        Menu::send_message(&mut player.stream, &format!("Your game code is {}. Please share this with other participants.\n", id));

        // create a chanel for the menu to talk with the game
        let (join_tx, join_rx) = mpsc::channel();
        self.current_games.insert(id, join_tx); 

        // create the player hash map
        let mut players = HashMap::new();
        players.insert(0, player);

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

    fn exist(&mut self, game_id: i32) -> bool {
        self.current_game_ids.contains(&game_id)
    }

    fn join_game(&mut self, player: Player, game_id: i32) -> Result<()> {
        // Get the game id
        for (id, game_tx) in &mut self.current_games {
            if (game_id == *id) {
                game_tx.send(player);
                return Ok(());
            }
        }

        Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid game code",
            ))
    }

    fn send_message(stream: &mut TcpStream, message: &str) -> Result<()> {
        stream.write_all(&format!("Menu: ").as_bytes());
        stream.write_all(&message.as_bytes())
    }
}

fn read_from_stream(stream: &TcpStream) -> Result<String> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    match reader.read_line(&mut line) {
        Ok(0) => {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client disconnected",
            ));
        }, // EOF reached
        Ok(message) => {
            Ok(line.clone().trim().to_string())
        }
        Err(e) => Err(e),
    }
}

fn player_listener(mut stream: TcpStream, tx: mpsc::Sender<PlayerRequest>, menu: Arc<Mutex<Menu>>) -> Result<()> {
    // get the game choice
    let choice : String = loop {
        Menu::send_message(&mut stream, "Welcome to The Resistance menu. If you want to join a game, enter '1' then hit enter. If you want to start a game, enter '2' then hit enter.\n")?;
        let mut message : String = read_from_stream(&stream)?;
        println!("Message: {}", message);
        if message == "1" {
            Menu::send_message(&mut stream, "You chose 1!\n")?;
            break message;
        } else if message == "2" {
            Menu::send_message(&mut stream, "You chose 2\n")?;
            break message;
        } else {
            Menu::send_message(&mut stream, "Invalid choice. Please choose again!\n")?;
        }
    };

    // handle the choice
    if choice == "2" {
        // Get number of players
        let player_num: usize = loop {
            let mut input = String::new();

            Menu::send_message(&mut stream, &format!("Please enter the number of player: "));
            
            let input = read_from_stream(&stream)?;

            let number : usize = input.trim().parse().unwrap();

            if number < 6 || number > 10 {
                Menu::send_message(&mut stream, &format!("The game is only for 6 to 10 players. Please try again!\n"));
                continue;
            } else {
                break number;
            }
        };

        // Get the player name and create the player
        Menu::send_message(&mut stream, &format!("What's your name? "));
        let player_name = read_from_stream(&stream)?;
        let new_player = Player { id: 0, name: player_name, stream: stream, faction: None, special_role: None};

        // get the mutex lock and initialize the game
        let mut shared_menu = menu.lock().unwrap();
        shared_menu.initialize_game(new_player, player_num);
    } else if choice == "1" {

        // Get the player name and create the player
        Menu::send_message(&mut stream, &format!("What's your name? "));
        let player_name = read_from_stream(&stream)?;
        let mut cloned_stream = stream.try_clone()?;
        let new_player = Player { id: 0, name: player_name, stream: stream, faction: None, special_role: None};

        // get the game code
        loop {
            Menu::send_message(&mut cloned_stream, &format!("What's the game code? "));
            let game_code : i32 = match read_from_stream(&cloned_stream)?.trim().parse() {
                Ok(game_code) => game_code,
                Err(err) => {
                    Menu::send_message(&mut cloned_stream, &format!("The game code is not valid, please try again!\n"));
                    continue
                },
            };
            

            let mut shared_menu = menu.lock().unwrap();
            if (shared_menu.exist(game_code)) {
                shared_menu.join_game(new_player, game_code);
                break
            } else {
                Menu::send_message(&mut cloned_stream, &format!("The game code is not valid, please try again!\n"));
            }
        }
        
    }
    
    Ok(())
}



fn main() -> Result<()> {    
    let tcp_endpoint = "0.0.0.0:5000";
    let listener = TcpListener::bind(tcp_endpoint)?;
    let (tx, rx) = mpsc::channel::<PlayerRequest>();

    let mut menu = Arc::new(Mutex::new(Menu {current_game_ids: HashSet::new(), current_games: HashMap::new()}));

    // spawn a handling thread for each incoming connection
    for incoming in listener.incoming() {
        let stream = incoming?;
        let tx_clone = tx.clone();
        let menu_clone = Arc::clone(&menu);

        thread::spawn(move || {
            player_listener(stream, tx_clone, menu_clone);
        });
    }

    println!("End!");

    Ok(())
}
