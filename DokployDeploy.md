# ICANN Downloader - Dokploy Deployment Rehberi

## Ön Gereksinimler

- Dokploy kurulu ve çalışır durumda
- ClickHouse zaten deploy edilmiş ve `domainengine-clickhouse-jbnfph` network'ünde
- ICANN CZDS hesabı (https://czds.icann.org)

---

## Adım 1: ClickHouse Kontrolü

ClickHouse'un çalıştığından ve doğru network'te olduğundan emin ol:

1. Dokploy'da ClickHouse service'ine git
2. Network'ün `domainengine-clickhouse-jbnfph` olduğunu doğrula
3. Service adının `clickhouse` olduğunu doğrula (Compose'daki service adı)

---

## Adım 2: Yeni Compose Service Oluştur

1. Dokploy Dashboard'a git
2. **"Create Service"** → **"Compose"** seç
3. **Source**: GitHub
4. **Repository**: `https://github.com/doctorcmptrmita2/IcanDownloader`
5. **Branch**: `main`

---

## Adım 3: Compose Dosyasını Yapıştır

Dokploy'da Compose editörüne şunu yapıştır:

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
      - domainengine-clickhouse-jbnfph
    labels:
      - traefik.enable=true
      - traefik.http.routers.icann-downloader.rule=Host(`icann.senin-domainin.com`)
      - traefik.http.routers.icann-downloader.entrypoints=web
      - traefik.http.services.icann-downloader.loadbalancer.server.port=8080

networks:
  domainengine-clickhouse-jbnfph:
    external: true
```

---

## Adım 4: Değişkenleri Düzenle

Compose dosyasında şunları değiştir:

| Değişken | Açıklama | Örnek |
|----------|----------|-------|
| `ICANN_USER` | ICANN CZDS kullanıcı adın | `user@email.com` |
| `ICANN_PASS` | ICANN CZDS şifren | `MySecretPass123` |
| `CLICKHOUSE_PASSWORD` | ClickHouse şifresi | `gk4wp30maukhmir56ytodgfl5i4i6l5s` |
| `Host(...)` | Traefik domain | `icann.example.com` |

---

## Adım 5: Deploy Et

1. **"Deploy"** butonuna tıkla
2. Build loglarını izle
3. Container'ın başladığını doğrula

---

## Adım 6: Logları Kontrol Et

Başarılı başlangıç logları şöyle görünmeli:

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

## Adım 7: Web Arayüzünü Test Et

Tarayıcıda domain'e git:
- `http://icann.senin-domainin.com`

Dashboard'da şunları göreceksin:
- Download durumu
- Son indirmeler
- Scheduler bilgisi
- Manuel download butonu

---

## Sorun Giderme

### "Temporary failure in name resolution" Hatası

**Sebep**: Container'lar farklı network'lerde

**Çözüm**: 
- Compose'da `networks` bölümünün doğru olduğundan emin ol
- ClickHouse ve icann-downloader aynı network'te olmalı

### "Database icann does not exist" Hatası

**Sebep**: Eski kod versiyonu

**Çözüm**: 
- Dokploy'da "Rebuild" yap
- En son kod database'i otomatik oluşturur

### "CZDSClient.__init__() got an unexpected keyword argument" Hatası

**Sebep**: Eski kod versiyonu

**Çözüm**: 
- Dokploy'da "Rebuild" yap

### Nginx Proxy Manager Sayfası Görünüyor

**Sebep**: Traefik routing yanlış

**Çözüm**: 
- `traefik.http.services.icann-downloader.loadbalancer.server.port=8080` label'ının olduğundan emin ol
- Domain'in doğru yazıldığından emin ol

---

## Environment Variables Referansı

| Değişken | Zorunlu | Varsayılan | Açıklama |
|----------|---------|------------|----------|
| `ICANN_USER` | ✅ | - | ICANN CZDS kullanıcı adı |
| `ICANN_PASS` | ✅ | - | ICANN CZDS şifresi |
| `DB_HOST` | ✅ | - | ClickHouse hostname |
| `CLICKHOUSE_PASSWORD` | ❌ | `""` | ClickHouse şifresi |
| `DB_PORT` | ❌ | `9000` | ClickHouse portu |
| `DB_NAME` | ❌ | `icann` | Database adı |
| `CRON_HOUR` | ❌ | `4` | Günlük download saati |
| `CRON_MINUTE` | ❌ | `0` | Günlük download dakikası |
| `PORT` | ❌ | `8080` | Web server portu |

---

## API Endpoints

| Endpoint | Method | Açıklama |
|----------|--------|----------|
| `/` | GET | Dashboard |
| `/api/status` | GET | Sistem durumu |
| `/api/logs` | GET | Son download logları |
| `/api/download/start` | POST | Manuel download başlat |
| `/api/scheduler/status` | GET | Scheduler durumu |

---

## Cron Schedule

Varsayılan olarak her gün saat **04:00**'da otomatik download başlar.

Değiştirmek için:
- `CRON_HOUR=4` → İstediğin saat (0-23)
- `CRON_MINUTE=0` → İstediğin dakika (0-59)
