# ICANN Downloader - Dokploy Sıfırdan Kurulum

Yeni Dokploy sunucusunda sıfırdan deployment.

---

## BÖLÜM 1: PROJE OLUŞTUR

1. Dokploy Dashboard'a gir
2. Sol menüden **"Projects"** tıkla
3. **"+ Create Project"** butonuna tıkla
4. **Name**: `icann-zone-downloader`
5. **"Create"** tıkla

---

## BÖLÜM 2: CLICKHOUSE SERVİSİ OLUŞTUR

1. Oluşturduğun projeye tıkla (`icann-zone-downloader`)
2. **"+ Create Service"** tıkla
3. **"Compose"** seç

### Compose Ayarları:
- **Service Name**: `clickhouse`

### Compose İçeriği:
Aşağıdaki YAML'ı yapıştır:

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse-db
    environment:
      - CLICKHOUSE_USER=default
      - CLICKHOUSE_PASSWORD=gk4wp30maukhmir56ytodgfl5i4i6l5s
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    restart: unless-stopped

volumes:
  clickhouse_data:
```

4. **"Deploy"** tıkla
5. Loglardan başarılı başladığını doğrula

---

## BÖLÜM 3: CLICKHOUSE NETWORK ADINI BUL

1. ClickHouse service'ine tıkla
2. **"Advanced"** sekmesine git
3. **"Networks"** bölümünü bul
4. Network adını not al (örnek: `icann-zone-downloader-clickhouse-abc123`)

> ⚠️ Bu network adını bir sonraki adımda kullanacaksın!

---

## BÖLÜM 4: ICANN DOWNLOADER SERVİSİ OLUŞTUR

1. Aynı projede **"+ Create Service"** tıkla
2. **"Compose"** seç

### Compose Ayarları:
- **Service Name**: `icann-downloader`

### Compose İçeriği:
Aşağıdaki YAML'ı yapıştır ve **DEĞİŞKENLERİ DÜZENLE**:

```yaml
services:
  icann-downloader:
    build:
      context: https://github.com/doctorcmptrmita2/IcanDownloader.git#main
    container_name: icann-downloader
    environment:
      - ICANN_USER=BURAYA_ICANN_KULLANICI_ADIN
      - ICANN_PASS=BURAYA_ICANN_SIFREN
      - DB_HOST=clickhouse
      - CLICKHOUSE_PASSWORD=gk4wp30maukhmir56ytodgfl5i4i6l5s
      - CRON_HOUR=4
      - CRON_MINUTE=0
    restart: unless-stopped
    networks:
      - BURAYA_CLICKHOUSE_NETWORK_ADI
    labels:
      - traefik.enable=true
      - traefik.http.routers.icann-downloader.rule=Host(`BURAYA_DOMAIN_YAZILACAK`)
      - traefik.http.routers.icann-downloader.entrypoints=web
      - traefik.http.services.icann-downloader.loadbalancer.server.port=8080

networks:
  BURAYA_CLICKHOUSE_NETWORK_ADI:
    external: true
```

### Değiştirilecek Yerler:

| Yer | Ne Yazılacak | Örnek |
|-----|--------------|-------|
| `BURAYA_ICANN_KULLANICI_ADIN` | ICANN CZDS email | `user@email.com` |
| `BURAYA_ICANN_SIFREN` | ICANN CZDS şifre | `MyPassword123` |
| `BURAYA_CLICKHOUSE_NETWORK_ADI` | Bölüm 3'te bulduğun network adı | `icann-zone-downloader-clickhouse-abc123` |
| `BURAYA_DOMAIN_YAZILACAK` | Traefik domain | `icann.example.com` |

> ⚠️ Network adı **2 yerde** değiştirilecek: `networks:` altında ve en alttaki `networks:` tanımında!

---

## BÖLÜM 5: ÖRNEK TAMAMLANMIŞ COMPOSE

Network adı `icann-zone-downloader-clickhouse-xyz789` olsun:

```yaml
services:
  icann-downloader:
    build:
      context: https://github.com/doctorcmptrmita2/IcanDownloader.git#main
    container_name: icann-downloader
    environment:
      - ICANN_USER=myemail@gmail.com
      - ICANN_PASS=MySecretPassword123
      - DB_HOST=clickhouse
      - CLICKHOUSE_PASSWORD=gk4wp30maukhmir56ytodgfl5i4i6l5s
      - CRON_HOUR=4
      - CRON_MINUTE=0
    restart: unless-stopped
    networks:
      - icann-zone-downloader-clickhouse-xyz789
    labels:
      - traefik.enable=true
      - traefik.http.routers.icann-downloader.rule=Host(`icann.mydomain.com`)
      - traefik.http.routers.icann-downloader.entrypoints=web
      - traefik.http.services.icann-downloader.loadbalancer.server.port=8080

networks:
  icann-zone-downloader-clickhouse-xyz789:
    external: true
```

---

## BÖLÜM 6: DEPLOY ET

1. **"Deploy"** butonuna tıkla
2. Build loglarını izle (2-3 dakika sürebilir)
3. Başarılı logları bekle:

```
INFO - Starting ICANN Downloader
INFO - Configuration loaded successfully
INFO - Connecting to ClickHouse at clickhouse:9000
INFO - Database 'icann' ensured to exist
INFO - Database tables initialized
INFO - ClickHouse connection successful
INFO - Services initialized successfully
INFO - Starting scheduler
INFO - Starting web server on port 8080
```

---

## BÖLÜM 7: DOMAIN AYARLA (Opsiyonel)

Eğer Traefik otomatik domain vermiyorsa:

1. icann-downloader service'ine git
2. **"Domains"** sekmesine tıkla
3. **"+ Add Domain"** tıkla
4. Domain gir veya Traefik'in verdiği domain'i kullan
5. **Port**: `8080`
6. **"Save"** tıkla

---

## BÖLÜM 8: TEST ET

1. Tarayıcıda domain'e git
2. Dashboard'u gör
3. **"Start Download"** butonuyla manuel test yap

---

## SORUN GİDERME

### Hata: "Temporary failure in name resolution"
**Çözüm**: Network adı yanlış. Bölüm 3'ü tekrar kontrol et.

### Hata: "Database icann does not exist"
**Çözüm**: Rebuild yap. Yeni kod otomatik oluşturur.

### Nginx Proxy Manager sayfası görünüyor
**Çözüm**: Traefik label'larını kontrol et, port 8080 olmalı.

### ClickHouse bağlantı hatası
**Çözüm**: 
- ClickHouse service'inin çalıştığından emin ol
- Network adının doğru olduğundan emin ol
- `DB_HOST=clickhouse` olmalı (service adı)

---

## ÖZET

| Adım | İşlem |
|------|-------|
| 1 | Proje oluştur: `icann-zone-downloader` |
| 2 | ClickHouse service oluştur |
| 3 | ClickHouse network adını bul |
| 4 | ICANN Downloader service oluştur |
| 5 | Değişkenleri düzenle |
| 6 | Deploy et |
| 7 | Test et |
