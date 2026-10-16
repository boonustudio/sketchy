# Sketchy (online draw and guess)

Node.js server (Express + WebSocket) with MongoDB Atlas. The server owns the words, timers, scoring and guess checking, so nobody can peek at the answer.

## Run locally
1. `npm install`
2. `cp .env.example .env` and paste your Atlas connection string into `MONGODB_URI`
3. `npm start`, then open http://localhost:3000 in two browser windows (or use the "Demo bots" button in the lobby).

## MongoDB Atlas checklist
- Database Access: create a user with read/write access.
- Network Access: allow your server's IP (or `0.0.0.0/0` while testing).
- Put the full `mongodb+srv://...` string in `MONGODB_URI`. If the password has special characters, URL-encode them.
- Collections are created automatically: `parties` (auto-deleted after 24h idle) and `games` (a record per finished game).
- If Atlas is unreachable the game still works, it just doesn't save anything. Check `/healthz` to see `"mongo": true`.

## Deploy
Needs a host that keeps a Node process and WebSockets open: Render, Railway, Fly.io, a VPS. Not Vercel/Netlify functions.
Set `MONGODB_URI` (and optionally `MONGODB_DB`) as environment variables. `PORT` is set by the host. The page auto-uses `wss://` over HTTPS.

## Notes
- Live rooms are in memory. After a server restart the host can rejoin the same code and the party reopens as a lobby (settings restored from MongoDB); other players rejoin with the code.
- Edit `words.txt` (comma or newline separated, no categories) to change the word list. About 4,500 words are included.
