> Dieses Projekt entwickelt sich schnell, und dieses Dokument kann bald veraltet sein. Die englische README im Projektverzeichnis ist in der Regel aktueller.

# pinetree

pinetree ist ein leichtgewichtiger, als einzelne Binärdatei ausgelieferter **Misskey-kompatibler Server für soziale Netzwerke**, geschrieben in Rust. Bestehende Misskey-Clients sprechen mit ihm über eine bewusst gewählte, vertraglich getestete Teilmenge der Misskey-API (plus Streaming), und er föderiert über ActivityPub mit Misskey, Sharkey und anderer Fediverse-Software.

Er ist bewusst **sparsam im Betrieb**: kein Postgres, kein Redis, kein Message Broker. Eine Binärdatei, eine eingebettete Datenbankdatei (`redb`), lokaler Dateispeicher, fertig.

pinetree bringt kein Frontend mit. Richte einen beliebigen Misskey-kompatiblen Webclient darauf ein — zum Beispiel [Keystone](https://github.com/estyseesghosts/keystone) (das pinetree als Backend ansteuert) oder [KissMe](https://github.com/estyseesghosts/kissme).

> **Status (2026-10-20):** der Single-Node-Server ist fertig bis hin zu Client-Kompatibilität, Föderation, Streaming, Zitierbeiträgen, verzweigten Antworten, Umfragen, Bearbeitung von Beiträgen, Entwürfen/Planung, Blockieren/Stummschalten, privaten Listen, Pins, Benachrichtigungsgruppierung/-filterung, Profilimport/-export und einem ersten Paket an Moderations- und Registrierungswerkzeugen. Multi-Node-Clustering (FoundationDB) ist weiterhin geplant, aber nicht gebaut. Details unten — einschließlich dessen, was noch *nicht* verdrahtet ist.

---

## Table of contents

- [What it does today](#what-it-does-today)
- [What it does not do](#what-it-does-not-do)
- [Known gaps and rough edges](#known-gaps-and-rough-edges)
- [Where things stand](#where-things-stand)
- [Run your own server](#run-your-own-server)
  - [Prerequisites](#prerequisites)
  - [Build it](#build-it)
  - [Start it](#start-it)
  - [Create your first account](#create-your-first-account)
  - [Log in and post](#log-in-and-post)
- [Use it day-to-day](#use-it-day-to-day)
- [What it talks to](#what-it-talks-to)
- [Project layout](#project-layout)
- [Testing it yourself](#testing-it-yourself)
- [Contributing & reading more](#contributing--reading-more)
- [License](#license)

---

## Was es heute kann

Alles in diesem Abschnitt läuft auf dem Single-Node-Server und ist durch ausgeführte Tests abgedeckt (siehe [Testing it yourself](#testing-it-yourself)). Rund 95 JSON-API-Endpunkte antworten unter `POST /api/{endpoint}`; alles, was nicht auf der Liste steht, liefert `404 UNKNOWN_API_ENDPOINT`, und unbekannte Felder bei gelisteten Endpunkten liefern `400 UNSUPPORTED_FIELD`. Der genaue Vertrag steht in `docs/endpoint-manifest.md`.

**Konten, Authentifizierung und Registrierung:**

- Registrierung / Anmeldung mit Berechtigungen pro Token, Ratenbegrenzungen im Misskey-Stil mit `429` + `retryAfter`.
- MiAuth-Webflow (`miauth/gen-token`, `GET|POST /miauth/{session}[/check]`) und das alte `auth/session/generate|show|userkey` + `auth/accept`.
- Registrierungsrichtlinie: offene Registrierung, nur auf Einladung (`invite/create|list|delete|limit`, `admin/invite/create|list`) oder adminverwaltet (`admin/accounts/create`, `disableRegistration` über `admin/meta` / `admin/update-meta`).
- Rollen: Admin und Moderator (`set-role`-CLI), auf Benutzern mitgeführte `isAdmin` / `isModerator`, ein Moderationsprotokoll (`admin/show-moderation-logs`).
- Sperrung (`admin/suspend-user|unsuspend-user`): Anmeldung wird mit `USER_SUSPENDED` abgelehnt, offene Streams werden geschlossen, Follows werden in begrenzten Durchläufen aufgelöst.
- Vollständiger Profilspeicher: `i/update` akzeptiert alle 39 Upstream-Profilparameter (Name, Beschreibung, Standort, Geburtstag, Avatar/Banner, followedMessage, Sprachen, stummgeschaltete Wörter/Instanzen, Empfangskonfiguration für Benachrichtigungen und mehr), plus pinetrees eigene Schalter `rejectQuotes` und `defaultReaction`.
- Selbstlöschung (`i/delete-account`) und Admin-Löschung (`admin/delete-account`), Admin-Benutzeransichten (`admin/show-user(s)`, `admin/update-user-note`).

**Beiträge und Timelines:**

- Erstellen (Text, Inhaltswarnung, Sichtbarkeit `public` / `home` / `followers` / `specified`, Antworten, Renotes, Dateianhänge, `localOnly`), Anzeigen, Löschen, Ent-Renoten.
- **Umfragen** mit Abstimmung (`notes/polls/vote`), Ablauf und Föderation als `Question` (+ `pollVoted`-Ereignisse).
- **Bearbeitung von Beiträgen** in der Sharkey-Form `notes/edit` (Antwort unveränderlich, Sichtbarkeit wird zurückgesetzt, Hinzufügen/Ersetzen einer Umfrage setzt Stimmen zurück, Versionsverlauf), plus `edited`-Benachrichtigungen und föderierte `Update(Note)`.
- **Zitierbeiträge** (FEP-044f kanonisch + Legacy-Kompatibilität + Anfrage-/Freigabe-Workflow; durchsetzung von quoterseitigem `rejectQuotes`, Fallback auf einfachen Beitrag bei entfernten Beiträgen) und **verzweigte Antwort-Threads**, die intakt bleiben, wenn der Elternbeitrag spät eintrifft (ausstehende Eltern bleiben geschlossen, bis der Resolver sie verknüpft).
- Home-/lokale/hybride/globale Timelines mit Cursor-Paginierung, `users/notes`, `notes/replies|renotes|children|conversation`, `notes/search|search-by-tag` (Teilzeichenkettensuche über lokale Beiträge), `notes/featured`, `users/featured-notes`.
- Durchsetzung zur Lesezeit überall: blockierte/stummgeschaltete/gesperrte Autoren, Instanzblockierungen und Renote-Stummschaltungen werden bei jeder Timeline und jedem Beitragslesezugriff herausgefiltert.

**Reaktionen, Follows und sozialer Graph:**

- Reaktionen erstellen/löschen mit Misskey-Normalisierung (nacktes-`❤`-Fallback, `likeOnly`, Legacy-Abbildung); `defaultReaction` pro Benutzer, sodass ein bloßes Herz-Antippen serverseitig aufgelöst wird; vollständige Reaktionsliste über `notes/reactions`.
- Folgen / Entfolgen, Folgeanfragen an gesperrte Konten (`following/requests/list|accept|reject|cancel`).
- Blockieren, Stummschalten, Renote-Stummschalten (`blocking/*`, `mute/*`, `renote-mute/*`, `users/relation`), durchgesetzt bei Follow/React/Vote/Erstellen und bei jedem Lesezugriff.
- Private Follower-/Folgen-Listen (Selbst-/Follower-/öffentliche Staffelung mit `FORBIDDEN` + Zählermaskierung) und private Reaktionen (`users/reactions`).
- Pins (`i/pin|unpin`, Limit 5, föderiert) und ein dokumentiertes „kompatibles, vereinfachtes“ Ranking für hervorgehobene Beiträge (nicht Upstream-identisch).

**Benachrichtigungen, Entwürfe, Import/Export:**

- `i/notifications` (Cursor, `markAsRead`, Einschluss-/Ausschluss-Typfilter, Empfangskonfiguration zum Erstellungszeitpunkt, Verwerfen bei Stummschaltung/eigenen Beiträgen), `i/notifications-grouped` (Reaktions-/Renote-Gruppierung, reine Pack-Schicht), `notifications/mark-all-as-read`.
- Entwürfe + geplante Beiträge (`notes/drafts/create|list|update|delete|count`; Planung = `scheduledAt` an einem Entwurf, veröffentlicht durch einen dauerhaften Job mit `scheduledNotePosted/Failed`-Benachrichtigungen).
- Profilimport/-export: 8 Exporte (`following`, `blocking`, `mute`, `notes`, `antennas`, `clips`, `favorites`, `user-lists`) + 5 Importe mit Upstream-Fehlercodes und einer `exportCompleted`-Benachrichtigung. (Entitäten, für die pinetree kein Modell hat — Benutzerlisten, Clips — exportieren leere gültige Dateien und importieren null Zeilen; Antennen und Favoriten exportieren/importieren echte Zeilen. Das ist dokumentiert, nicht stillschweigend.)

**Streaming:**

- Authentifizierter WebSocket `/streaming` (`?i=` oder Bearer): `main`, `homeTimeline`, `localTimeline`, `hybridTimeline`, `globalTimeline`, `antenna` (eigentümerbezogene `params.antennaId`, `note`-Frames bei Treffern).
- Beitragserfassung (`subNote`/`unsubNote` + Aliase, `reacted`/`unreacted`/`deleted` mit der Sichtbarkeitsprüfung), `readNotification`, `pollVoted`, `edited`-Beitragsaktualisierungen, unbekannte Kanäle werden mit `UNSUPPORTED_CHANNEL` beantwortet.
- Sockets gesperrter Benutzer werden serverseitig geschlossen.

**Medien:**

- Multipart-/JSON-Upload (`drive/files/create`), Auflisten/Anzeigen/Aktualisieren/Löschen, öffentliche Auslieferung unter `GET /files/{id}`, lokaler Dateisystem-Blob-Speicher.
- Entfernte Anhänge werden validiert und zwischengespeichert (Schema, SSRF, Größenlimit, MIME-Erkennung). Keine Ordner: `folderId` muss null sein.

**Föderation (ActivityPub):**

- WebFinger (JRD + XRD), host-meta, NodeInfo 2.0/2.1, stabile Akteurdokumente mit RSA-Schlüsseln, Outbox-/Followers-/Following-/Featured-Sammlungen, Beitragsdokumente (Tombstone-`410` für gelöschte).
- Geteilte + persönliche Inboxen. Digest- + Legacy-HTTP-Signaturen (`rsa-sha256`) eingehend geprüft und ausgehend gesendet; 64-KiB-Körpergrenze, ±5 Min. `Date`-Toleranz, Akteur-/Objekt-Autoritätsprüfungen, atomare Aktivitäts-ID-Deduplizierung (in derselben Transaktion wie die Zustandsänderung).
- Aktivitäten: Follow / Accept / Reject / Undo, Create(Note), Update (Bearbeitungen und Umfragestimmen), Delete, Like mit `_misskey_reaction`, Announce, `Question` (Umfragen), FEP-044f-Zitierautorisierung auf dem pinetree↔pinetree-Pfad. Eingehendes `EmojiReact` (Akkoma/Pleroma-Familie) wird über den Like-Pfad angenommen.
- Dauerhafte ausgehende Zustellung mit exponentiellem Backoff, Limits pro Ziel, Deduplizierung über geteilte Inboxen; endgültig fehlgeschlagene Jobs werden mit protokolliertem Grund quarantäniert, statt ewig erneut versucht zu werden.
- Instanzblockierungen (`blockedHosts`): subdomänenbewusst, eingehend (Verwerfen vor dem Schreibvorgang) und zum Zustellzeitpunkt ausgehend durchgesetzt.
- Föderationseingaben werden als nicht vertrauenswürdig behandelt: Größen-/Timeout-/Weiterleitungslimits, SSRF-Schutz (private IPs standardmäßig blockiert), HTML-/MFM-Bereinigung.

**Haltbarkeit:**

- Jede lokale Zustandsänderung wird zusammen mit ihrem Outbox-/Job-Datensatz in einer Speichertransaktion committet; Worker übernehmen Benachrichtigungen, Streaming-Fan-out und Zustellung. Jobs sind idempotent, dedupliziert, wiederholbar und absturzsicher (Absturz-Wiederherstellungstests).

---

## Was es nicht tut

Bewusste Nicht-Ziele (Proposal §1) und aufgeschobene Arbeit — wer danach fragt, erhält `404 UNKNOWN_API_ENDPOINT` (API) oder `UNSUPPORTED_CHANNEL` (Streaming) statt einer kaputten Halbimplementierung:

- **Kein Multi-Node-Modus.** `--mode=cluster` verweigert den Start (`cluster mode is not implemented (Phase 4)`). Ein Prozess, eine eingebettete `redb`-Datei, kein FoundationDB-Adapter, kein Kontenverzeichnis, kein knotenübergreifendes Routing. Das ist Phase 4, nicht begonnen.
- **Kein mitgelieferter Webclient.** Nutze Keystone, KissMe oder einen beliebigen Misskey-kompatiblen Client.
- **Keine Mastodon-Client-REST-API.** ActivityPub-Föderation *mit* Servern der Mastodon-Familie bleibt im Umfang; die Mastodon-API selbst ist ein Nicht-Ziel (Sharkey liefert eine aus; pinetree wird das nie tun, außer das Proposal ändert sich).
- **Benutzerdefinierte Emoji sind instanzkuratiert.** Öffentliche `emojis`-/`emoji`-Auflistung, `admin/emoji/add|update|delete|list` (Moderator), Kurzcode-Registry mit Groß-/Kleinschreibung-unabhängiger Eindeutigkeit, Remote-Emoji-Opt-in pro Benutzer (`showRemoteEmojis`, Metadatenaufnahme + verzögertes Nachladen). Fremde `:shortcode:`-Reaktionen werden unverändert gespeichert. Ausgehende Reaktionen an Hosts der Akkoma/Pleroma-Familie fallen auf ein einfaches `Like` (❤-Favorit) zurück, bis eine `EmojiReact`-Sendeform einen Live-Rundlauftest besteht.
- **Benutzerlisten ausgeliefert (Batch O WP16).** `users/lists/*` (12 Endpunkte), `notes/user-list-timeline` (nur lesend durch den Eigentümer), `userList`-Streaming-Kanal (`userAdded`/`userRemoved`/Beiträge), Empfangskonfigurationen für Listenbenachrichtigungen, CSV-Export/-Import, und Antennen-`src=list` löst jetzt über den Mitgliedschaftsindex auf. Limits durchgesetzt + beworben (`userListLimit 10`, `userEachUserListsLimit 50`). Entfernte Mitglieder reihen einen System-Akteur-Follow ein. `create-from-public` bleibt 404.
- **Keine Clips, Kanäle, Seiten, Drive-Ordner, Chat, Reversi oder Suche-nach-Hashtag.** Keine hervorgehobenen Timelines, kein `drive`-Streaming-Kanal, kein `signin-flow`, keine 2FA/Passkeys/Webhooks. Antennen, Favoriten, benutzerdefinierte Emoji, Benutzerlisten, Erwähnungs-Timeline, Benachrichtigungshilfsfunktionen, Discovery, Versionen und Übersetzungsstubs sind ausgeliefert (Batch N + O + P).
- **Kein OAuth.** Nur MiAuth + API-Token (OAuth kommt nur, falls ein Zielclient es erfordert).
- **Kein Thread-Stummschalten, keine RFC-9421- oder Linked-Data-Signaturen,** kein `_misskey_content` (wird per Entscheidung nie gesendet), keine Umfragen über gelöschte/abgelaufene Grenzfälle hinaus über dokumentierte Fehler hinaus. Kontenumzug (`i/move` + eingehendes `Move`) ist in Batch P enthalten — live zwischen zwei pinetrees verifiziert.
- **Selbstbedienung, Follower-Entfernung, Remote-Lookup, Drive-Extras und URL-Vorschau ausgeliefert (Batch O WP9/WP10/WP12/WP13).** `i/change-password`, `i/regenerate-token` (native Rotation; App-Berechtigungen bleiben bestehen), `i/apps`, `i/authorized-apps`, echtes `i/revoke-token{tokenId}`, `i/signin-history` (nur Vorwärts-Zeilen ab Deployment, begrenzt auf 100/Benutzer), `following/invalidate` (vom Gefolgten verfasstes Reject an entfernte Follower), `users/search-by-username-and-host`, `ap/get` (zugangsbeschränkte sichere Teilmenge von `ap/show`), `drive/files/upload-from-url`, `drive/files/find-by-hash`, `url-preview` aus erster Hand.
- **E-Mail-Identitäts- + Discovery-Paket ausgeliefert (Batch P).** `i/update-email`, `request-reset-password`, `reset-password`, `verify-email` (codevollständig, ruhend: kein SMTP konfiguriert — Versand schlägt laut fehl, nie still), `i/move` + eingehendes `Move` (Alias-Handshake, Follower-Migration, Wiederholungs-/Fälschungsabdeckung), `i/registry/*` (8 Endpunkte), `get-online-users-count` (Streaming-Präsenzdefinition), `pinned-users` (`[]`), `fetch-external-resources`, `fetch-rss` (dokumentierte Teilmenge), `server-info`, `stats` (echte Zählungen), `notes/mentions`, `notifications/test-notification`, `notifications/flush` (Als-gelesen-Markieren + Ereignis), `users/recommendation`, `notes/recommendations` (vereinfachte Bewertung), `notes/versions` (Sharkey-Form), `notes/translate` (ruhend: kein Backend — lauter Fehler).
- **Missbrauchsmeldungen sind verdrahtet** (`users/report-abuse` + die `admin/abuse-user-reports`-Familie mit `Flag`-Weiterleitung); Admins fragen die Liste ab (keine Mail-/Webhook-Benachrichtigungen).

---

## Bekannte Lücken und Ecken und Kanten

Dinge, die funktionieren, aber mit dokumentierten Grenzen, über die man stolpern kann:

- **Nur Loopback-getestet.** Interop ist gegen echte Server in lokalen Laboren verifiziert (siehe unten), nie gegen das live öffentliche Internet. SSRF-Schutz blockiert private IPs standardmäßig; Loopback-Föderation braucht `PINETREE_ALLOW_PRIVATE_IPS=1`.
- **Single-Node-Speicher.** Ein Schreiber (`redb`), keine Lesereplikate, noch kein Live-Migrationspfad zu Multi-Node (Export/Import deckt Konten, Follows, Blockierungen, Stummschaltungen und Beiträge ab — kein transparentes Upgrade).
- **Suche ist eine begrenzte Teilzeichenkettensuche** über zwischengespeicherte Beiträge, kein Index. `notes/reactions` blättert neueste-zuerst nur mit `sinceId`-Cursorn (dokumentierte Abweichung von Misskeys aufsteigender Reihenfolge).
- **Hervorgehobene Beiträge nutzen ein vereinfachtes Ranking** (aktuelle nicht-sensitive öffentliche Beiträge, bewertet nach Reaktionen + Renotes) — in der Form kompatibel, nicht identisch zu Upstream.
- **Akteur-RSA-Schlüssel werden bei erster Verwendung verzögert erzeugt** — langsam in Debug-Builds, sodass der erste Remote-Abruf eines neuen Kontos bei Peers in einen Timeout laufen kann.
- **Wort-Stummschaltungen werden serverseitig durchgesetzt** (eigentümergenehmigte Abweichung von Upstream, das sie nur clientseitig durchsetzt): `hardMutedWords` verwerfen Beiträge bei jedem Lesezugriff; `mutedWords` packen ein betrachterbezogenes `isSoftMuted`-Flag (nur wahr bei Treffer und wenn der Beitrag keine eigene CW hat). Siehe `docs/word-mutes.md`.
- **Private Folgelisten haben noch keine Sichtbarkeitsregler pro Benutzer** — das Profil speichert die Felder, Listen staffeln nach der dokumentierten Regel (selbst/Follower/öffentlich).
- **Keine Zustellungsfeinabstimmung pro Ziel, keine Timeline-Indizes jenseits des Reaktionen-nach-Beitrag-Index, minimale Beobachtbarkeit** (nur stderr-Diagnostik). Das ist Phase-5-Arbeit und beginnt erst mit Messungen.
- **Benchmarks sind Debug-Build-Single-Node-Zahlen** (~870 Beitragserstellungen/s, ~430 Timeline/s, ~1200 Benutzeranzeigen/s) — ein grober Anhaltspunkt, kein Versprechen.

---

## Wo das Projekt steht

| Phase | Was es war | Status |
|---|---|---|
| 0 | Verträge einfrieren (API-Manifest, Identitätsregeln, Speichertests) | Fertig |
| 1 | Single-Node-Server (Konten, Beiträge, Follows, Reaktionen, Drive, Jobs) | Fertig |
| 2 | Föderation (WebFinger, Akteure, Signaturen, Inboxen, Zustellung, Dedup) | Fertig |
| 3 | Client-Kompatibilität & Streaming (Live-WebSocket, reichere API, Echt-Client-Vertragstests) | Fertig |
| — | Zitierbeiträge (FEP-044f) und verzweigte Antworten obendrauf | Fertig |
| — | Misskey-Paritätspaket: Umfragen, Bearbeitung, Entwürfe/Planung, Blockierungen/Stummschaltungen, private Listen, Pins, Benachrichtigungsgruppierung/-filterung, Import/Export | Fertig |
| — | Moderationspaket (Batch M, vor Phase 4): Rollen, Registrierung/Einladungen, Sperrung, Instanzblockierungen, Admin-Ansichten; Missbrauchsmelde-Verkabelung unterhalb der API-Schicht | Größtenteils fertig — WP1–WP6 gelandet, WP7+ ausstehend |
| — | Alltagspaket (Batch N): Favoriten + Beitragszustand, benutzerdefinierte Emoji (+ Remote-Opt-in), `followRequestAccepted`, Antennen, Missbrauchsmelde-Routen, Admin-Rest, Wort-Stummschaltungen (serverseitig), MiAuth-Härtung, Sharkey-Planungs-Shim | Fertig (diese Sitzung) — `url-preview`, Selbstbedienung, Follower-Entfernung, Benutzerlisten noch ausstehend |
| 4 | Multi-Node auf FoundationDB | Geplant — nicht begonnen |
| 5 | Skalierung & Härtung | Geplant |

Der Single-Node-Server ist **heute lauffähig**. Der Multi-Node-Modus verweigert den Start, bis Phase 4 landet (das ist Absicht).

---

## Eigenen Server betreiben

### Voraussetzungen

1. **Rust** (aktuell stabil). Hole es dir von [rustup](https://rustup.rs/), falls du es nicht hast.
2. Das war's für den Serverbetrieb selbst.

> Wenn du später die vollständige Testsuite oder die Interop-Labore ausführen willst, brauchst du außerdem `bun`, Docker (mit dem Compose-Plugin), Node 24 und Node 22. Aber um ein *pinetree zu betreiben*, brauchst du nur Rust.

### Bauen

```bash
cargo build --release
```

Das erzeugt eine einzelne Binärdatei unter `target/release/pinetree`.

> **Falls du WSL nutzt:** halte deinen Checkout auf dem nativen Linux-Dateisystem (z. B. `/home/you/pinetree`, nicht `/mnt/c/...`). Das 9P-Dateisystem ist um ein Mehrfaches langsamer für Builds und für die Datenbank. Du kannst auch ein benutzerdefiniertes Zielverzeichnis in `.cargo/config.toml` (git-ignoriert) setzen, wenn du die Build-Artefakte auf einem schnellen Laufwerk haben willst.

### Starten

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  --listen=127.0.0.1:3000
```

- `--public-url` ist die URL, die die Welt (und andere Server) sehen wird. Sie muss mit `http://` oder `https://` beginnen und einen Host enthalten. Akteur-URLs, Beitrags-URLs und WebFinger werden alle daraus gebaut.
- `--data-dir` ist der Ort, an dem die eingebettete Datenbank (`pinetree.redb`) und hochgeladene Dateien liegen. Zeige auf ein Verzeichnis, das du sichern kannst.
- `--listen` ist die Adresse, an die der HTTP-Server bindet. Nutze `127.0.0.1:3000`, um ihn nur lokal zu halten, oder `0.0.0.0:3000`, um ihn in deinem Netzwerk freizugeben.

Wenn du es nur lokal ausprobierst, geht auch das:

```bash
target/release/pinetree \
  --public-url=http://127.0.0.1:3000 \
  --data-dir=./data \
  --listen=127.0.0.1:3000
```

> Standardmäßig sind Föderationsanfragen an private/interne IPs blockiert (SSRF-Schutz). Für reines lokales Föderationstesten zwischen Servern auf deiner Maschine kannst du dich mit `PINETREE_ALLOW_PRIVATE_IPS=1` dafür entscheiden. In Produktion willst du die Blockierung eingeschaltet lassen.

### Erstes Konto erstellen

Du kannst Konten von der Kommandozeile aus erstellen:

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  create-account --username=alice --password=a-good-password
```

(Ja, der Unterbefehl `create-account` steht nach den Server-Flags — er beendet sich nach dem Erstellen des Kontos, statt den Server zu starten. Die Registrierungsrichtlinie — offen, nur auf Einladung oder nur durch Admins — wird danach über `admin/update-meta` und die Einladungsendpunkte verwaltet.)

Um dich selbst zum Admin (oder Moderator) zu machen:

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  set-role --username=alice --admin
```

(`--moderator`, und `=false` zum Entziehen, funktionieren genauso.)

### Anmelden und posten

pinetree unterstützt mehrere Anmeldewege:

**Über die API (signin):**

```bash
curl -s -X POST http://127.0.0.1:3000/api/signin \
  -H 'content-type: application/json' \
  -d '{"username":"alice","password":"a-good-password"}'
```

Das liefert `{"i": "your-api-token"}` zurück. Nutze dieses Token als `i` in jeder API-Anfrage oder als `Authorization: Bearer <token>`.

**Über das Web (MiAuth):** öffne `http://127.0.0.1:3000/miauth/<session>` in einem Browser, nachdem du mit `POST /api/miauth/gen-token` ein Token erzeugt hast.

**Über einen Misskey-Client:** richte deinen liebsten Misskey-kompatiblen Client auf die URL deines Servers ein und melde dich mit Benutzername/Passwort von oben an.

Dann poste einen Beitrag:

```bash
curl -s -X POST http://127.0.0.1:3000/api/notes/create \
  -H 'content-type: application/json' \
  -H 'Authorization: Bearer your-api-token' \
  -d '{"text":"hello from pinetree","visibility":"public"}'
```

---

## Nutzung im Alltag

Sobald er läuft, interagierst du mit pinetree genauso wie mit jeder Misskey-Instanz:

- **Posten:** `POST /api/notes/create` mit `text`, optional `visibility`, `cw` (Inhaltswarnung), `replyId`, `renoteId`, `fileIds`, `localOnly`, `poll`. Bearbeiten mit `notes/edit`, abstimmen mit `notes/polls/vote`.
- **Timelines:** `POST /api/notes/timeline`, `notes/local-timeline`, `notes/hybrid-timeline`, `notes/global-timeline`; Thread-Ansichten über `notes/replies|renotes|children|conversation`; `notes/search`, `notes/search-by-tag`.
- **Reaktionen:** `POST /api/notes/reactions/create` mit `noteId` + `reaction` (z. B. `"👍"`); lasse `reaction` weg, um deine `defaultReaction` zu verwenden.
- **Follows:** `POST /api/following/create` mit `userId`; gesperrte Konten laufen über `following/requests/*`. Blockieren/Stummschalten über `blocking/*`, `mute/*`, `renote-mute/*`.
- **Entwürfe:** `notes/drafts/create|list|update|delete|count`; füge `scheduledAt` hinzu, um später zu veröffentlichen.
- **Portabilität:** `i/export-*` / `i/import-*` für Follows, Blockierungen, Stummschaltungen, Beiträge und mehr.
- **Streaming:** verbinde einen WebSocket mit `ws://<host>/streaming?i=<token>` (oder mit Bearer-Header). Kanäle umfassen `main`, `homeTimeline`, `localTimeline`, `hybridTimeline` und `globalTimeline`.
- **Drive:** `POST /api/drive/files/create` (multipart) zum Hochladen; Dateien werden unter `GET /files/<id>` ausgeliefert.

Die vollständige API-Oberfläche ist in `docs/endpoint-manifest.md` (der eingefrorene Vertrag plus datierte ergänzende Hinweise) und `docs/phase-3-profile.md` (das Kompatibilitätsprofil) dokumentiert. Alles, was nicht in dieser Liste steht, liefert `404 UNKNOWN_API_ENDPOINT`.

---

## Womit es spricht

pinetree föderiert über ActivityPub und wurde gegen echte Software in lokalen (Loopback-/Docker-)Laboren getestet — bisher nie gegen das öffentliche Internet:

- **Misskey** (2026.10.1-alpha.0) und **Sharkey** (2025.4.7): Follow-, Beitrags-, Reaktions-, Antwort- und Zitierflüsse sowie Zustellung in einem live Vier-Server-Interop-Labor (zwei pinetrees + echtes Misskey + echtes Sharkey).
- **Akkoma / Pleroma-Familie, Mastodon, Catodon und eine Referenzimplementierung:** breiteres Mehrfamilien-Bench-Harness (`scripts/fediverse-bench/`) für Follows, Funktions-Interop (Beiträge, Umfragen, Reaktionen, Zitate) und Lastläufe. Bekannte bleibende Grenzen aus diesen Läufen: ausgehende Emoji-Reaktionen an Hosts der Akkoma-Familie fallen auf Favoriten zurück; fremde benutzerdefinierte Emoji werden nie gerendert.

Es spricht die Kern-Aktivitätstypen (Follow, Accept, Reject, Undo, Create, Update, Delete, Like, Announce, Question), WebFinger, NodeInfo, HTTP-Signaturen und FEP-044f-Zitierautorisierung (voller Workflow zwischen pinetrees; legacy-kompatibel mit Misskey/Sharkey).

---

## Projektaufbau

```
.
├── sns-server-proposal.md    the product & architecture plan (source of truth)
├── AGENTS.md                 rules for anyone (human or AI) working in this repo
├── docs/                     frozen contracts, phase plans, exit criteria, wiki
│   ├── endpoint-manifest.md  the Misskey API contract (+ superseding notes)
│   ├── phase-3-profile.md    the compatibility profile (target clients, scope)
│   ├── misskey-parity-plan.md  the parity batch (§§0–13 done; Batch M status)
│   ├── implementer-guide-batch-m.md  moderation/daily-use batch (WP1–WP6 done)
│   ├── testing.md            how to run every test suite
│   └── wiki/                 living overview docs (About, Current State, Roadmap)
├── src/                      the Rust crate
│   ├── domain/               pure types & rules (no HTTP, no DB)
│   ├── application/          use cases (commands, queries, ports)
│   ├── api/misskey/          HTTP API adapter (endpoints, packers, errors)
│   ├── activitypub/          ActivityPub protocol adapter
│   ├── federation/           inbox, outbox, delivery, retry
│   ├── persistence/redb/     the embedded database adapter
│   ├── streaming/            WebSocket server & channel hub
│   ├── jobs/                 durable job queue & worker
│   ├── media/                file blob storage
│   └── cluster/              Phase 4 multi-node (stub for now)
├── tests/                    conformance (100 cases incl. polls, edits,
│                             blocks, imports), federation, streaming,
│                             crash-recovery, real-client smoke tests
├── scripts/                  smoke-all.sh and friends (one-command test gates)
├── scripts/fediverse-bench/  multi-family federation bench harness
├── state/logs/               dated work logs with real command output
└── misskey, sharkey          symlinks to reference checkouts (git-ignored;
                               only needed for tests, not for running a server)
```

Der Code ist ein **modularer Monolith**: ein Crate, klare Abhängigkeitsrichtung (`domain` ← `application` ← Adapter). Die Misskey-API und die ActivityPub-Adapter rufen *dieselben* Anwendungsoperationen auf — keine doppelte Geschäftslogik.

---

## Selbst testen

Das vollständige Verifikationsgate ist:

```bash
bash scripts/smoke-all.sh
```

Das führt (in Reihenfolge) aus:

1. `cargo test --offline` — die vollständige Rust-Testsuite. Aktuell **553 grün**: 424 Unit-Tests, 100 Speicher-Konformanzfälle auf redb und 29 Integrationstests (Streaming über live WebSockets, Föderation zwischen In-Prozess-Knoten, Absturz-Wiederherstellung und echte-`misskey-js`-Client-Smoketests).
2. `scripts/smoke-phase1.sh` — Kernflüsse, angesteuert durch die echte `misskey-js`-Clientbibliothek.
3. `scripts/smoke-phase3.sh` — die Kompatibilitätsprofil-Endpunkte und Streaming über `misskey-js`.
4. `scripts/keystone-sweep.sh` — die exakten API-Aufrufe des Keystone-Frontends (41 Prüfungen, alle bestehend).
5. `scripts/smoke-phase2.sh` — live Vier-Server-Föderations-Interop (echtes Misskey + echtes Sharkey + zwei pinetree-Instanzen). Braucht Node 22 für die Sharkey-Seite; ohne sie wird sauber übersprungen.

Du kannst die Teile auch einzeln ausführen:

```bash
cargo test --offline                          # Rust tests only
PINETREE_BIN=target/release/pinetree bash scripts/smoke-phase1.sh
```

Siehe `docs/testing.md` für die vollständige Anleitung, einschließlich der manuellen Ausführung des Interop-Rigs und des Mehrfamilien-Bench-Harness.

---

## Beitragen & weiterlesen

- `sns-server-proposal.md` — das Produkt- und Architekturproposal. Gewinnt bei jeder Abweichung von dieser README.
- `AGENTS.md` — die Spielregeln für Mitwirkende (Mensch oder KI).
- `docs/` — die eingefrorenen Verträge und das lebendige Wiki.
- `state/logs/` — datierte Arbeitsprotokolle mit echten Ausgaben. Lies den neuesten Eintrag, um auf den Stand zu kommen.

Wenn du helfen willst, ist das Nützlichste: es ausprobieren, die Testsuite ausführen und Issues anlegen, wenn etwas nicht funktioniert. Der Batch-M-Rest (WP7+: Favoriten, Emoji, Selbstbedienung, Follower-Entfernung, URL-Vorschau, Drive-Extras) und dann Phase 4 (Multi-Node) sind die offenen Baustellen.

---

## Lizenz

Noch nicht festgelegt — es gibt noch keine `LICENSE`-Datei. Frage nach, bevor du etwas annimmst.
