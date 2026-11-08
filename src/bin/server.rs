use std::net::{TcpStream, TcpListener};
use std::os::unix::fs::PermissionsExt;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;
use std::thread;
use rand::Rng;
use rand::seq::IndexedRandom;

#[derive(Eq, Hash, PartialEq, Debug, Clone, Copy)]
enum SpecialRole {
    Merlin,
    Assassin
}

#[derive(Eq, Hash, PartialEq, Debug, Clone, Copy)]
enum Faction {
    ResistanceOperative,
    GovernmentSpy,
}

trait Role {
    fn as_string(&self) -> String;
}

impl Role for Faction{
    fn as_string(&self) -> String {
        match self {
            Faction::ResistanceOperative => String::from("Resistance Operative"),
            Faction::GovernmentSpy => String::from("Goverment Spy"),
        }
    }
}

impl Role for SpecialRole{
    fn as_string(&self) -> String {
        match self {
            SpecialRole::Merlin => String::from("Merlin"),
            SpecialRole::Assassin => String::from("Assassin"),
        }
    }
}

#[derive(PartialEq)]
enum GamePhase {
    Connect,
    SpyReview,
    LeaderPick,
    TeamDiscuss, // 2 mins
    TeamVote, // 1 min
    QuestTime,
    ResistanceWin,
    GovermentWin
}

enum Recipient {
    Everyone,
    Faction(Faction),
    Id(i32),
    PlayerIds(Vec<i32>),
    SpecialRole(SpecialRole)
}


#[derive(Debug)] 
struct Player {
    id: i32,
    name: String,
    stream: TcpStream,
    faction: Option<Faction>,
    special_role: Option<SpecialRole>
}

struct Game {
    players: HashMap<i32, Player>,
    assassin_id: i32,
    merlin_id: i32,
    game_phase: GamePhase,
    resist_op_c: usize,
    gov_spy_c: usize,
    succeeded_task: i32
}

impl Game {
    fn get_player_name(&self, id: &i32) -> Option<String> {
        match self.players.get(id) {
            Some(player) => { Some(player.name.clone()) },
            None => { None }
        }
    }

    fn get_mut_player(&mut self, id: &i32) -> &mut Player {
       self.players.get_mut(id).expect("Player not found")
    }

    fn get_all_players_names(&self) -> String {
        let mut player_names : Vec<String> = Vec::new();

        for (id, player) in &self.players {
            player_names.push(player.name.clone());
        }

        return player_names.join(",");
    }

    fn assign_roles(&mut self) {
        let mut rng = rand::rng();
        let mut choices: Vec<i32> = self.players.keys().copied().collect();

        // Pick Government spies
        let gov_spy_ids: Vec<i32> = choices.sample(&mut rng, self.gov_spy_c).copied().collect();
        let assassin_id = gov_spy_ids.choose(&mut rng).copied().expect("Empty vector");

        for id in &gov_spy_ids {
            let mut player = self.get_mut_player(id);
            player.faction = Some(Faction::GovernmentSpy);
        }

        let mut assasin_player = self.get_mut_player(&assassin_id);
        assasin_player.special_role = Some(SpecialRole::Assassin);
        self.assassin_id = assassin_id;

        // remove the gov spies from choices
        choices.retain(|x| !gov_spy_ids.contains(x));
        
        let resist_op_ids = choices;
        let merlin_id = resist_op_ids.choose(&mut rng).copied().expect("Empty vector");

        for id in resist_op_ids {
            let mut player = self.get_mut_player(&id);
            player.faction = Some(Faction::ResistanceOperative);
        }

        let mut merlin_player = self.get_mut_player(&merlin_id);
        merlin_player.special_role = Some(SpecialRole::Merlin);
        self.merlin_id = merlin_id;
    }

    fn send_message(&mut self, recipient: Recipient, message: &str) {
        for (id, player) in &mut self.players {
            let is_recipient : bool = match &recipient {
                Recipient::Everyone => true,
                Recipient::Faction(f) => {
                    player.faction == Some(*f)
                },
                Recipient::Id(id) => {
                    player.id == *id
                },
                Recipient::PlayerIds(ids) => {
                    ids.contains(id)
                }
                Recipient::SpecialRole(role) => {
                    player.special_role == Some(*role)
                }
            };

            if is_recipient {
                writeln!(player.stream, "{}", message);
            }
        }
    }

    fn is_leader_message_valid(&self, message: &str) -> Option<Vec<i32>> {
        let mut chosen_team_ids : Vec<i32> = Vec::new();

        let chosen_names: Vec<&str> = message.split(',').collect();

        for name in chosen_names {
            let mut is_valid = false;
            for (id, player) in &self.players {
                if player.name == name {
                    chosen_team_ids.push(*id);
                    is_valid = true;
                    break;
                }
            }

            if !is_valid {
                return None;
            }
        }

        return Some(chosen_team_ids)
    }

    fn is_team_vote_valid(&self, has_vote: &mut i32, player_id: &i32, message: &str) -> i32 {
        let bit_mask = 1 << player_id;
        if (*has_vote & bit_mask) >= 1 {
            return 0;
        }

        if message == "yes" {
            println!("Has vote: {has_vote:b}");
            *has_vote |= bit_mask;
            println!("Has vote: {has_vote:b}");
            1
            
        } else if message == "no" {
            println!("Has vote: {has_vote:b}");
            *has_vote |= bit_mask;
            println!("Has vote: {has_vote:b}");
            -1
        } else {
            0
        }
    }

    // return the id of the player picked by assassin
    fn is_assasin_vote_valid(&self, message: &str) -> Option<i32> {
        for (id, player) in &self.players {
            if player.name == message {
                return Some(*id);
            }
        }

        return None;
    }
}

fn handle_player(stream: TcpStream, id: i32, tx: mpsc::Sender<(i32, String)>) {
    // read from player stream
    
    let reader = BufReader::new(stream);

    // get message and send it to rx
    for line in reader.lines() {
        match line {
            Ok(message) => {
                if tx.send((id, message)).is_err() {
                    break;
                }
            }
            Err(e) => {
                println!("{}", format!("Player {} disconnected: {e}", id));
                break;
            }
        }
    }

}

fn main() -> std::io::Result<()> {
    // ==== INITIALIZE GAME ====
    let tcp_endpoint = "0.0.0.0:5000";

    let listener = TcpListener::bind(tcp_endpoint)?;

    // Get number of players
    let player_num: usize = loop {
        let mut input = String::new();

        println!("Please enter the number of player:");
        io::stdin().read_line(&mut input)?;

        let number : usize = input.trim().parse().unwrap();

        if number < 6 || number > 10 {
            println!("The game is only for 6 to 10 players. Please try again!");
            continue;
        } else {
            break number;
        }
    };

    let mut game = Game { players: HashMap::new(), game_phase: GamePhase::Connect, resist_op_c: player_num - 3, gov_spy_c: 3, succeeded_task: 0, assassin_id: -1, merlin_id: -1};
    
    // ==== PHASE 0: CONNECTING PLAYERS ====
    let mut player_count : usize = 0;
    let mut player_id : i32 = 0;
    println!("Waiting for players to connect...");

    while player_count < player_num {
        let (mut stream, _) = listener.accept()?;
        let mut buffer = [0u8; 1024];
        let n = stream.read(&mut buffer)?;

        let player_name = String::from_utf8_lossy(&buffer[..n]).trim().to_owned();
        
        println!("{player_name} connected!");

        let message = format!("Welcome to the game, {player_name}!");
        stream.write_all(message.as_bytes())?;

        let new_player_id : i32 = player_id;
        player_count += 1;
        player_id += 1;
        let new_player = Player { id: new_player_id, name: player_name, stream: stream, faction: None, special_role: None};
        game.players.insert(new_player_id, new_player);
    }

    // Assign roles for player
    game.assign_roles();

    // ==== PHASE 1: ROLE ANNOUCEMENT ====
    let role_messages: Vec<(i32, String)> = game.players.iter().map(|(id, player)| {
        let role_message = match player.special_role {
            Some(role) => format!("Your role is {}.", role.as_string()),
            None => match player.faction {
                Some(faction) => format!("Your role is {}.", faction.as_string()),
                None => panic!("Invalid faction!"),
            },
        };

        (*id, role_message)
    }).collect();

    for (id, message) in role_messages {
        game.send_message(Recipient::Id(id), &message);
    }

    // Spawn listener thread
    let (tx, rx) = mpsc::channel::<(i32, String)>(); // the thread return a tuple of id and message
    
    for (id, player) in &game.players {
        let tx = tx.clone();
        let stream = player.stream.try_clone().unwrap();
        let id = *id;
        thread::spawn(move || {
            handle_player(stream, id, tx);
        });
    }

    thread::spawn(move || {
        let mut input = String::new();

        loop {
            input.clear();
            io::stdin().read_line(&mut input);
            tx.send((-1, input.clone()));
        }
    });

    // ==== PHASE 2: GOVERNMENT SPIES MEET UP ====
    let gov_spies_name: String = game
        .players
        .iter()
        .filter_map(|(_, player)| {
            if player.faction == Some(Faction::GovernmentSpy) {
                Some(player.name.clone())
            } else {
                None
            }
        })
        .collect::<Vec<String>>()
        .join(", ");

    let message = format!("Hey spies, this is your teammate: {gov_spies_name}");
    game.send_message(Recipient::Faction(Faction::GovernmentSpy), &message);

    let message = format!("Her Merlin, these are the spy team: {gov_spies_name}");
    game.send_message(Recipient::SpecialRole(SpecialRole::Merlin), &message);


    // ===== PHASE 3: STARTING THE TASKS ====

    let mut leader = 0;
    let player_count = player_count as i32;
    for task in 0..5 {
        if game.game_phase == GamePhase::GovermentWin || game.game_phase == GamePhase::ResistanceWin {
            break;
        }
        game.send_message(Recipient::Everyone, &format!("\n== START TASK {} ==", task+1));
        // === 3.1: LEADER PICK THE TEAM ===
        game.game_phase = GamePhase::LeaderPick;

        let mut picked_team : Vec<i32> = Vec::new();
        let mut picked_team_names = String::from("");

        for pick_team_attempt in 0..5 {
            game.send_message(Recipient::Everyone, &format!("\nPick team attempt number {}. Leader: {}", pick_team_attempt+1, game.get_player_name(&leader).unwrap()));
            game.send_message(Recipient::Id(leader), &format!("Hey leader, please enter a list of name for the team with format: 'name1,name2,name3'. Your options are: {}", game.get_all_players_names()));

            loop {
                match rx.recv() {
                    Ok((player_id, message)) => {
                        println!("Player_id: {}", player_id);
                        println!("Message: {}", message);

                        if player_id == leader {
                            let team_list = match game.is_leader_message_valid(&message) {
                                Some(list) => {
                                    picked_team = list.clone();
                                    picked_team_names = message.clone();
                                    break;
                                },
                                None => {
                                    let message = "Hey leader, wrong format. Please enter a list of name for the team with format: 'name1,name2,name3'";
                                    game.send_message(Recipient::Id(leader), &message);
                                    continue;
                                }
                            };
                            
                        }
                    },
                    Err(RecvError) => break
                }
            }

            // === 3.2: TEAM DISCUSS ON THE QUEST MEMBER ===
            game.game_phase = GamePhase::TeamDiscuss;
            let new_message = format!("Everyone, this is the quest team that the leader picked: {}. Please start the discussion. You'll have 2 minutes to discuss.", picked_team_names);
            game.send_message(Recipient::Everyone, &new_message);

            // The team will have 2 mins to discuss 
            loop {
                match rx.recv_timeout(Duration::from_secs(120)) {
                    Ok((player_id, message)) => {
                        println!("Player_id: {}", player_id);
                        println!("Message: {}", message);
                        if player_id == -1 {
                            if message.contains("Skip") {
                                break;
                            }
                            else {
                                continue;
                            }
                        }
                        let m = format!("{}: {}", game.get_player_name(&player_id).unwrap(), &message);
                        game.send_message(Recipient::Everyone, &m);
                    },
                    Err(RecvTimeoutError::Timeout) => {
                        game.send_message(Recipient::Everyone, "Discussion time is up");
                        break;
                    },
                    Err(RecvTimeoutError::Disconnected) => {
                        println!("All senders disconnected");
                        break;
                    }
                }
            }

            // The team have 1 min to vote
            let mut vote_result : i32 = 0;
            game.send_message(Recipient::Everyone, "Everyone please put it 1 word for your vote as 'yes' or 'no'. You'll have 1 minute!");
            let mut has_vote : i32 = 0;
            loop {
                match rx.recv_timeout(Duration::from_secs(120)) {
                    Ok((player_id, message)) => {
                        // let vote_player_id = player_id.clone();
                        println!("Has vote: {has_vote:b}");
                        let vote = game.is_team_vote_valid(&mut has_vote, &player_id, &message);
                        if vote == 0 {
                            game.send_message(Recipient::Id(player_id), "Invalid vote or you already vote");
                        }
                        vote_result += vote;
                        if has_vote == (1 << player_count) - 1 {
                            break;
                        }
                    },
                    Err(RecvTimeoutError::Timeout) => {
                        game.send_message(Recipient::Everyone, "Discussion time is up");
                        break;
                    },
                    Err(RecvTimeoutError::Disconnected) => {
                        println!("All senders disconnected");
                        break;
                    }
                }
            }

            if vote_result > 0 {
                game.send_message(Recipient::Everyone, &format!("The majority agreed about the Quest team. Let's move to the Quest phase!"));
                break;
            } else {
                game.send_message(Recipient::Everyone, &format!("The majority disagreed about the Quest team."));
                if pick_team_attempt == 4 {
                    game.send_message(Recipient::Everyone, &format!("This is the 5th failed pick team attempt. The Goverment Spy won!"));
                    game.game_phase = GamePhase::GovermentWin;
                    break;
                } else {
                    game.send_message(Recipient::Everyone, &format!("Let's pick again!"));
                }
            }
        }
        
        // ==== PHASE 3.3: QUEST TEAM START TO VOTE ====
        game.send_message(Recipient::Everyone, "Quest team started to vote.");
        game.send_message(Recipient::PlayerIds(picked_team.clone()), "Hey Quest team, please vote 'yes' if you want this quest to succeed, otherwise vote 'no'. We need at least 2 votes for the quest to succeed. You'll have 1 minute.");

        let mut has_vote = 0;
        let mut bit_mask = 0;
        let mut vote_result : i32 = 0;
        for id in &picked_team {
            bit_mask |= 1 << id;
        }
        println!("bit mask: {bit_mask:b}");
        loop {
            match rx.recv_timeout(Duration::from_secs(60)) {
                Ok((player_id, message)) => {
                    if picked_team.contains(&player_id) {
                        println!("Has vote: {has_vote:b}");
                        let vote = game.is_team_vote_valid(&mut has_vote, &player_id, &message);
                        if vote == 0 {
                            game.send_message(Recipient::Id(player_id), "Invalid vote or you already vote");
                        }
                        vote_result += vote;
                        if has_vote == bit_mask {
                            break;
                        }
                    }
                    
                },
                Err(RecvTimeoutError::Timeout) => {
                    game.send_message(Recipient::Everyone, "Discussion time is up");
                    break;
                },
                Err(RecvTimeoutError::Disconnected) => {
                    println!("All senders disconnected");
                    break;
                }
            }
        }

        if vote_result > 0 {
            game.send_message(Recipient::Everyone, &format!("The quest has succeeded. Congrats the Resistance!"));
            game.succeeded_task += 1;
        } else {
            game.send_message(Recipient::Everyone, &format!("The quest has failed. Good job Government Spies!"));
        }

        leader = (leader + 1) % player_count;
    }

    // ==== PHASE 4: ASSASSIN TRY TO KILL MERLIN
    game.send_message(Recipient::Everyone, &format!("The current succeeded task is {}/5. Assassin, please put in the name of the person who you think is Merlin.", game.succeeded_task));
    game.send_message(Recipient::SpecialRole(SpecialRole::Assassin), &format!("Assassin, your valid votes are {}. You will have 1 minute.", game.get_all_players_names()));

    loop {
        match rx.recv_timeout(Duration::from_secs(60)) {
            Ok((player_id, message)) => {
                if player_id == game.assassin_id {
                    match game.is_assasin_vote_valid(&message) {
                        Some(id) => {
                            if id == game.merlin_id {
                                game.send_message(Recipient::Everyone, "Assassin has killed Merlin!");
                                game.game_phase = GamePhase::GovermentWin;
                            } else {
                                game.send_message(Recipient::Everyone, "Assassin has missed the shot!");
                            };
                            break;
                        },
                        None => {
                            game.send_message(Recipient::SpecialRole(SpecialRole::Assassin), "Invalid input, please try again!");
                        }
                    }
                }
            },
            Err(RecvTimeoutError::Timeout) => {
                game.send_message(Recipient::SpecialRole(SpecialRole::Assassin), "Decided time is up!");
                break;
            },
            Err(RecvTimeoutError::Disconnected) => {
                println!("All senders disconnected");
                break;
            }
        }
    }

    game.send_message(Recipient::Everyone, &format!("The number of succeeded task is {}/5!", game.succeeded_task));
    if game.game_phase != GamePhase::GovermentWin {
        if game.succeeded_task >= 3 {
            game.game_phase = GamePhase::ResistanceWin;
        } else {
            game.game_phase = GamePhase::GovermentWin;
        }
    }

    match game.game_phase {
        GamePhase::ResistanceWin => {
            game.send_message(Recipient::Everyone, "Resistance Operative team has won!");
        },
        GamePhase::GovermentWin => {
            game.send_message(Recipient::Everyone, "Government Spy team has won!");
        },
        _ => ()
    }
    
    Ok(())
}

