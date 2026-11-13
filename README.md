# The resistance

The game server is running on Digital Ocean. You can go to: http://201.79.40.73:3000/ to play the game or connect to the server through netcat like displayed below.

1. Connect to the game
```shell
nc 201.79.40.73 5000
```

2. Choose the option: join a game or create a game
![alt text](readme_img/image.png)

3. If you want to join a game (Option 1), please enter your name and a valid game code
![alt text](readme_img/image2.png)

4. If you want to create a game (Option 2), enter number of people and your name. You'll receive a game code to share with other participants.
![alt text](readme_img/image3.png)

5. Wait for everyone in the party to arrive so you can start the game.

## Known vulnerability

1. The port 3000 is exposed right now, im working to move it to nginx
2. The tcp packet is non-encrypted
3. There's a race condition to connect to a game even if it's is full in the game.