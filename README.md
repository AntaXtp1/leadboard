# 24h Nürburgring Live Timing

## Cara Jalanin

### Local (paling gampang)
```bash
pip install -r requirements.txt
python main.py
# buka http://localhost:8080
```

### Google Cloud Shell
1. Upload 3 file ini ke Cloud Shell (drag & drop)
2. Jalanin:
```bash
pip install -r requirements.txt --break-system-packages
python main.py
```
3. Klik tombol **Web Preview** → Port 8080

---

## Debug
Kalau leaderboard kosong (data posisi belum keparse), scroll ke bawah
ada section **"Raw WebSocket packets"** — expand itu dan kirim ke gue,
kita lihat PID mana yang bawa data posisi.
