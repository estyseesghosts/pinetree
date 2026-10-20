> Ce projet évolue vite et ce document peut rapidement devenir obsolète. Le README anglais à la racine du projet est généralement plus à jour.

# pinetree

pinetree est un **serveur de réseau social compatible Misskey** léger et
monobinaire, écrit en Rust. Les clients Misskey existants s'y connectent via
un sous-ensemble délibérément choisi et testé par contrat de l'API Misskey
(plus le streaming), et il fédère en ActivityPub avec Misskey, Sharkey et
d'autres logiciels du fediverse.

Il est délibérément **économe en ressources** : ni Postgres, ni Redis, ni
courtier de messages. Un seul binaire, un seul fichier de base de données
embarquée (`redb`), un stockage de fichiers local, et c'est tout.

pinetree ne fournit pas d'interface web. Pointez n'importe quel client web
compatible Misskey vers lui — par exemple
[Keystone](https://github.com/estyseesghosts/keystone) (qui cible pinetree
comme backend) ou [KissMe](https://github.com/estyseesghosts/kissme).

> **État (2026-10-20) :** le serveur mono-nœud est terminé jusqu'à la
> compatibilité client, la fédération, le streaming, les citations de notes,
> les réponses branchées, les sondages, l'édition des notes, les
> brouillons/planifications, les blocages/mises en sourdine, les listes
> privées, les épingles, le regroupement/filtrage des notifications, ainsi
> qu'un premier lot d'outils de modération et d'inscription. Le
> multi-nœud (FoundationDB) reste prévu, pas encore construit. Détails
> ci-dessous — y compris ce qui n'est *pas* encore branché.

---

## Table des matières

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

## Ce qu'il fait aujourd'hui

Tout dans cette section fonctionne sur le serveur mono-nœud et est couvert
par des tests exécutés (voir [Testing it yourself](#testing-it-yourself)).
Environ 95 points d'API JSON répondent à `POST /api/{endpoint}` ; tout ce qui
ne figure pas dans la liste renvoie `404 UNKNOWN_API_ENDPOINT`, et les champs
inconnus sur les points listés renvoient `400 UNSUPPORTED_FIELD`. Le contrat
exact est `docs/endpoint-manifest.md`.

**Comptes, authentification et inscription :**

- Inscription / connexion avec permissions par jeton, limites de débit façon
  Misskey avec `429` + `retryAfter`.
- Flux web MiAuth (`miauth/gen-token`, `GET|POST /miauth/{session}[/check]`)
  et `auth/session/generate|show|userkey` + `auth/accept` historiques.
- Politique d'inscription : inscription ouverte, sur invitation seulement
  (`invite/create|list|delete|limit`, `admin/invite/create|list`), ou gérée
  par les admins (`admin/accounts/create`, `disableRegistration` via
  `admin/meta` / `admin/update-meta`).
- Rôles : admin et modérateur (CLI `set-role`), `isAdmin` / `isModerator`
  inclus dans les utilisateurs, journal de modération
  (`admin/show-moderation-logs`).
- Suspension (`admin/suspend-user|unsuspend-user`) : connexion refusée avec
  `USER_SUSPENDED`, flux ouverts fermés, suivis démantelés par balayages
  bornés.
- Magasin de profils complet : `i/update` accepte les 39 paramètres de profil
  amont (nom, description, localisation, anniversaire, avatar/bannière,
  followedMessage, langues, mots/instances en sourdine, configuration de
  réception des notifications, et plus), plus les bascules propres à pinetree
  `rejectQuotes` et `defaultReaction`.
- Auto-suppression (`i/delete-account`) et suppression par un admin
  (`admin/delete-account`), vues admin des utilisateurs (`admin/show-user(s)`,
  `admin/update-user-note`).

**Notes et fils :**

- Création (texte, avertissement de contenu, visibilité `public` / `home` /
  `followers` / `specified`, réponses, renotes, pièces jointes, `localOnly`),
  affichage, suppression, annulation de renote.
- **Sondages** avec votes (`notes/polls/vote`), expiration, et fédération en
  `Question` (+ événements `pollVoted`).
- **Édition des notes** au format Sharkey `notes/edit` (réponse immuable,
  visibilité ramenée de force, ajout/remplacement de sondage réinitialisant
  les votes, historique des versions), plus notifications `edited` et
  `Update(Note)` fédéré.
- **Citations de notes** (canonique FEP-044f + compatibilité historique +
  flux de demande/approbation ; application côté citant de `rejectQuotes`,
  repli distant en note simple avec suppression de la citation) et **fils de
  réponses branchées** qui restent intacts quand le parent arrive en retard
  (parents en attente fermés par défaut jusqu'à ce que le résolveur les lie).
- Fils d'accueil / local / hybride / global avec pagination par curseur,
  `users/notes`, `notes/replies|renotes|children|conversation`,
  `notes/search|search-by-tag` (balayage par sous-chaîne sur les notes
  locales), `notes/featured`, `users/featured-notes`.
- Application systématique des règles à la lecture : auteurs
  bloqués/mis en sourdine/suspendus, instances bloquées et auteurs en
  sourdine de renotes sont filtrés sur chaque fil et chaque lecture de note.

**Réactions, suivis et graphe social :**

- Création/suppression de réactions avec normalisation Misskey (repli `❤`
  nu, `likeOnly`, correspondance historique) ; `defaultReaction` par
  utilisateur pour qu'un simple toucher du cœur soit résolu côté serveur ;
  liste complète des réactions via `notes/reactions`.
- Suivi / désuivi, demandes de suivi vers les comptes verrouillés
  (`following/requests/list|accept|reject|cancel`).
- Blocage, mise en sourdine, sourdine de renotes (`blocking/*`, `mute/*`,
  `renote-mute/*`, `users/relation`), appliqués sur les
  suivis/réactions/votes/créations et sur chaque lecture.
- Listes privées d'abonnés/abonnements (filtrage soi / abonné / public avec
  `FORBIDDEN` + masquage des compteurs) et réactions privées
  (`users/reactions`).
- Épingles (`i/pin|unpin`, limite de 5, fédérées) et classement documenté des
  notes en vedette « compatible, simplifié » (non identique à l'amont).

**Notifications, brouillons, import/export :**

- `i/notifications` (curseur, `markAsRead`, filtres de types
  include/exclude, règles de configuration de réception à la création,
  suppressions sourdine/soi-même), `i/notifications-grouped`
  (regroupement réactions/renotes, pure couche d'emballage),
  `notifications/mark-all-as-read`.
- Brouillons + notes planifiées (`notes/drafts/create|list|update|delete|count` ;
  la planification = `scheduledAt` sur un brouillon, publié par un travail
  durable avec notifications `scheduledNotePosted/Failed`).
- Import/export de profil : 8 exports (`following`, `blocking`, `mute`,
  `notes`, `antennas`, `clips`, `favorites`, `user-lists`) + 5 imports avec
  les codes d'erreur amont et une notification `exportCompleted`. (Pour les
  entités sans modèle dans pinetree — listes d'utilisateurs, clips — export
  de fichiers vides valides et import de zéro ligne ; les antennes et les
  favoris exportent/importent de vraies lignes. C'est documenté, pas
  silencieux.)

**Streaming :**

- WebSocket authentifié `/streaming` (`?i=` ou Bearer) : `main`,
  `homeTimeline`, `localTimeline`, `hybridTimeline`, `globalTimeline`,
  `antenna` (`params.antennaId` limité au propriétaire, trames `note` pour
  les correspondances).
- Capture de notes (`subNote`/`unsubNote` + alias, `reacted`/`unreacted`/`deleted`
  avec la règle de visibilité), `readNotification`, `pollVoted`, mises à jour
  de notes `edited`, canaux inconnus répondus par `UNSUPPORTED_CHANNEL`.
- Les sockets des utilisateurs suspendus sont fermées côté serveur.

**Médias :**

- Envoi multipart/JSON (`drive/files/create`), listage/affichage/mise à
  jour/suppression, service public à `GET /files/{id}`, stockage binaire sur
  le système de fichiers local.
- Les pièces jointes distantes sont validées et mises en cache (schéma,
  SSRF, limite de taille, reniflage MIME). Pas de dossiers : `folderId` doit
  être null.

**Fédération (ActivityPub) :**

- WebFinger (JRD + XRD), host-meta, NodeInfo 2.0/2.1, documents d'acteur
  stables avec clés RSA, collections
  outbox/followers/following/featured, documents de notes (pierre tombale
  `410` pour les supprimées).
- Boîtes partagées + personnelles. Signatures HTTP Digest + historiques
  (`rsa-sha256`) vérifiées en entrée et émises en sortie ; plafond de corps
  de 64 Kio, écart de `Date` de ±5 min, contrôles d'autorité
  acteur/objet, déduplication atomique des ID d'activité (dans la même
  transaction que le changement d'état).
- Activités : Follow / Accept / Reject / Undo, Create(Note), Update (éditions
  et votes de sondage), Delete, Like avec `_misskey_reaction`, Announce,
  `Question` (sondages), autorisation de citation FEP-044f sur le chemin
  pinetree↔pinetree. L'`EmojiReact` entrant (famille Akkoma/Pleroma) est
  accepté via le chemin Like.
- Livraison sortante durable avec recul exponentiel, limites par destination,
  déduplication des boîtes partagées ; les travaux en échec définitif sont
  mis en quarantaine avec une raison journalisée au lieu de réessayer sans fin.
- Blocages d'instances (`blockedHosts`) : sensibles aux sous-domaines,
  appliqués en entrée (rejet avant écriture d'état) et à la livraison en
  sortie.
- Les entrées de fédération sont traitées comme non fiables :
  limites de taille/délais/redirections, protection SSRF (IP privées
  bloquées par défaut), assainissement HTML/MFM.

**Durabilité :**

- Chaque changement d'état local est validé avec son enregistrement
  outbox/travail dans une seule transaction de stockage ; des travailleurs
  s'occupent des notifications, de la diffusion du streaming et de la
  livraison. Les travaux sont idempotents, dédupliqués, réessayables et sûrs
  en cas de crash (tests de reprise sur crash).

---

## Ce qu'il ne fait pas

Non-objectifs délibérés (proposition §1) et travaux différés — demander ces
fonctionnalités renvoie `404 UNKNOWN_API_ENDPOINT` (API) ou
`UNSUPPORTED_CHANNEL` (streaming) plutôt qu'une implémentation à moitié
cassée :

- **Pas de mode multi-nœud.** `--mode=cluster` refuse de démarrer
  (`cluster mode is not implemented (Phase 4)`). Un seul processus, un seul
  fichier `redb` embarqué, pas d'adaptateur FoundationDB, pas d'annuaire de
  comptes, pas de routage inter-nœuds. C'est la phase 4, non commencée.
- **Pas de client web fourni.** Utilisez Keystone, KissMe ou tout client
  compatible Misskey.
- **Pas d'API REST cliente Mastodon.** La fédération ActivityPub *avec* les
  serveurs de la famille Mastodon reste dans le périmètre ; l'API Mastodon
  elle-même est un non-objectif (Sharkey en fournit une ; pinetree n'en aura
  jamais sauf si la proposition change).
- **Les émojis personnalisés sont gérés par l'instance.** Listage public
  `emojis`/`emoji`, `admin/emoji/add|update|delete|list` (modérateur),
  registre de shortcodes avec unicité insensible à la casse, opt-in des
  émojis distants par utilisateur (`showRemoteEmojis`, ingestion de
  métadonnées + récupération paresseuse). Les réactions `:shortcode:`
  étrangères sont stockées telles quelles. Les réactions sortantes vers les
  hôtes de la famille Akkoma/Pleroma se dégradent en simple `Like` (favori ❤)
  jusqu'à ce qu'un format d'émission `EmojiReact` passe un test aller-retour
  en direct.
- **Listes d'utilisateurs livrées (Batch O WP16).** `users/lists/*` (12
  points), `notes/user-list-timeline` (lecture tirée réservée au
  propriétaire), canal de streaming `userList`
  (`userAdded`/`userRemoved`/notes), configurations de réception des
  notifications de liste, export/import CSV, et `src=list` des antennes se
  résout désormais via l'index d'appartenance. Limites appliquées +
  annoncées (`userListLimit 10`, `userEachUserListsLimit 50`). Les membres
  distants mettent en file un Follow de l'acteur système.
  `create-from-public` reste en 404.
- **Ni clips, ni canaux, ni pages, ni dossiers du drive, ni chat, ni
  Reversi, ni recherche par hashtag.** Ni fils en vedette, ni canal de
  streaming `drive`, ni `signin-flow`, ni 2FA/clés d'accès/webhooks. Les
  antennes, favoris, émojis personnalisés, listes d'utilisateurs, fil de
  mentions, utilitaires de notification, découverte, versions et bouchons de
  traduction sont livrés (Batch N + O + P).
- **Pas d'OAuth.** MiAuth + jetons d'API seulement (OAuth arrivera uniquement
  si un client cible l'exige).
- **Ni sourdine de fils, ni signatures RFC 9421 ou Linked-Data,** ni
  `_misskey_content` (jamais émis, par décision), ni sondages sur
  l'angle supprimé/expiré au-delà des erreurs documentées. Le déménagement
  de compte (`i/move` + `Move` entrant) est livré dans le Batch P — vérifié
  en direct entre deux pinetrees.
- **Libre-service, suppression d'abonné, recherche distante, extras du
  drive et aperçu d'URL livrés (Batch O WP9/WP10/WP12/WP13).**
  `i/change-password`, `i/regenerate-token` (rotation native ; les
  autorisations d'applications survivent), `i/apps`, `i/authorized-apps`,
  vrai `i/revoke-token{tokenId}`, `i/signin-history` (lignes déployées vers
  l'avant, plafonnées à 100/utilisateur), `following/invalidate` (Reject
  rédigé par le suivi vers les abonnés distants),
  `users/search-by-username-and-host`, `ap/get` (sous-ensemble sûr
  authentifié de `ap/show`), `drive/files/upload-from-url`,
  `drive/files/find-by-hash`, `url-preview` de première partie.
- **Lot identité e-mail + découverte livré (Batch P).**
  `i/update-email`, `request-reset-password`, `reset-password`,
  `verify-email` (code complet, dormant : pas de SMTP configuré — les envois
  échouent bruyamment, jamais silencieusement), `i/move` + `Move` entrant
  (poignée de main d'alias, migration des abonnés, rejeu/falsification
  couverts), `i/registry/*` (8 points), `get-online-users-count`
  (définition par présence streaming), `pinned-users` (`[]`),
  `fetch-external-resources`, `fetch-rss` (sous-ensemble documenté),
  `server-info`, `stats` (vrais compteurs), `notes/mentions`,
  `notifications/test-notification`, `notifications/flush` (marquage lu +
  événement), `users/recommendation`, `notes/recommendations` (score
  simplifié), `notes/versions` (format Sharkey), `notes/translate`
  (dormant : pas de backend — échec bruyant).
- **Les signalements d'abus sont branchés** (`users/report-abuse` + la
  famille `admin/abuse-user-reports` avec retransmission `Flag`) ; les
  admins consultent la liste (pas de notifications mail/webhook).

---

## Lacunes connues et aspérités

Choses qui fonctionnent mais avec des limites documentées sur lesquelles vous
risquez de buter :

- **Testé en boucle locale seulement.** L'interopérabilité est vérifiée
  contre de vrais serveurs dans des labos locaux (voir ci-dessous), jamais
  contre l'internet public en direct. La protection SSRF bloque les IP
  privées par défaut ; la fédération en boucle locale nécessite
  `PINETREE_ALLOW_PRIVATE_IPS=1`.
- **Stockage mono-nœud.** Un seul écrivain (`redb`), pas de réplicas de
  lecture, pas encore de chemin de migration à chaud vers le multi-nœud
  (l'export/import couvre comptes, suivis, blocages, sourdes et notes — pas
  une montée de version transparente).
- **La recherche est un balayage borné par sous-chaîne** sur les notes en
  cache, pas un index. `notes/reactions` pagine du plus récent au plus ancien
  sur des curseurs `sinceId` uniquement (divergence documentée par rapport à
  l'ordre croissant de Misskey).
- **Les notes en vedette utilisent un classement simplifié** (notes publiques
  récentes non sensibles, scorées par réactions + renotes) — compatible en
  forme, pas identique à l'amont.
- **Les clés RSA des acteurs sont générées paresseusement au premier usage**
  — lent dans les builds de débogage, donc la première récupération distante
  d'un nouveau compte peut expirer pour les pairs.
- **Les sourdes de mots sont appliquées côté serveur** (divergence approuvée
  par le propriétaire par rapport à l'amont, qui les applique côté client
  seulement) : `hardMutedWords` écarte les notes à chaque lecture ;
  `mutedWords` joint un drapeau `isSoftMuted` propre au lecteur (vrai
  seulement en cas de correspondance et si la note n'a pas son propre CW).
  Voir `docs/word-mutes.md`.
- **Les listes privées de suivis n'ont pas encore de réglages de visibilité
  par utilisateur** — le profil stocke les champs, les listes appliquent la
  règle documentée (soi/abonné/public).
- **Pas de réglage de livraison par destination, pas d'index de fils au-delà
  de l'index réactions-par-note, observabilité minimale** (diagnostics stderr
  seulement). C'est du travail de phase 5 et ça ne commencera que sur
  mesures.
- **Les repères sont des chiffres mono-nœud en build de débogage** (~870
  créations de notes/s, ~430 fils/s, ~1200 users-show/s) — un ordre de
  grandeur, pas une promesse.

---

## Où en est le projet

| Phase | De quoi il s'agissait | État |
|---|---|---|
| 0 | Figer les contrats (manifeste API, règles d'identité, tests de stockage) | Terminé |
| 1 | Serveur mono-nœud (comptes, notes, suivis, réactions, drive, travaux) | Terminé |
| 2 | Fédération (WebFinger, acteurs, signatures, boîtes, livraison, dédup) | Terminé |
| 3 | Compatibilité client & streaming (WebSocket en direct, API enrichie, tests de contrat en vrai client) | Terminé |
| — | Citations de notes (FEP-044f) et réponses branchées par-dessus | Terminé |
| — | Lot parité Misskey : sondages, édition, brouillons/planification, blocages/sourdes, listes privées, épingles, regroupement/filtrage des notifications, import/export | Terminé |
| — | Lot modération (Batch M, pré-phase 4) : rôles, inscription/invitations, suspension, blocages d'instances, vues admin ; plomberie des signalements d'abus sous la couche API | Plutôt terminé — WP1–WP6 livrés, WP7+ en attente |
| — | Lot usage quotidien (Batch N) : favoris + état des notes, émojis personnalisés (+ opt-in distant), `followRequestAccepted`, antennes, routes de signalement d'abus, reste admin, sourdes de mots (côté serveur), durcissement MiAuth, cale de planification Sharkey | Terminé (cette session) — `url-preview`, libre-service, suppression d'abonné, listes d'utilisateurs encore en attente |
| 4 | Multi-nœud sur FoundationDB | Prévu — non commencé |
| 5 | Passage à l'échelle & durcissement | Prévu |

Le serveur mono-nœud est **prêt à tourner aujourd'hui**. Le mode
multi-nœud refuse de démarrer tant que la phase 4 n'est pas livrée (c'est
voulu).

---

## Héberger votre propre serveur

### Prérequis

1. **Rust** (stable récent). Prenez-le sur [rustup](https://rustup.rs/) si
   vous ne l'avez pas.
2. C'est tout pour faire tourner le serveur lui-même.

> Si vous voulez lancer la suite de tests complète ou les labos
> d'interopérabilité plus tard, il vous faudra aussi `bun`, Docker (avec le
> plugin compose), Node 24 et Node 22. Mais pour *faire tourner un
> pinetree*, seul Rust suffit.

### Le compiler

```bash
cargo build --release
```

Cela produit un seul binaire à `target/release/pinetree`.

> **Si vous êtes sous WSL :** gardez votre copie sur le système de fichiers
> Linux natif (p. ex. `/home/you/pinetree`, pas `/mnt/c/...`). Le système
> 9P est plusieurs fois plus lent pour les builds et pour la base de
> données. Vous pouvez aussi définir un répertoire cible personnalisé dans
> `.cargo/config.toml` (ignoré par git) si vous voulez les artefacts de
> build sur un disque rapide.

### Le démarrer

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  --listen=127.0.0.1:3000
```

- `--public-url` est l'URL que le monde (et les autres serveurs) verra. Elle
  doit commencer par `http://` ou `https://` et inclure un hôte. Les URL
  d'acteurs, les URL de notes et WebFinger seront tous construits à partir
  d'elle.
- `--data-dir` est l'endroit où vivent la base embarquée (`pinetree.redb`)
  et les fichiers envoyés. Pointez-le vers un répertoire que vous pouvez
  sauvegarder.
- `--listen` est l'adresse sur laquelle le serveur HTTP écoute. Utilisez
  `127.0.0.1:3000` pour le garder local uniquement, ou `0.0.0.0:3000` pour
  l'exposer sur votre réseau.

Si vous l'essayez juste en local, ceci suffit aussi :

```bash
target/release/pinetree \
  --public-url=http://127.0.0.1:3000 \
  --data-dir=./data \
  --listen=127.0.0.1:3000
```

> Par défaut, les requêtes de fédération vers les IP privées/internes sont
> bloquées (protection SSRF). Pour des tests de fédération locale entre
> serveurs sur votre machine, vous pouvez l'autoriser avec
> `PINETREE_ALLOW_PRIVATE_IPS=1`. En production, gardez le blocage activé.

### Créer votre premier compte

Vous pouvez créer des comptes depuis la ligne de commande :

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  create-account --username=alice --password=a-good-password
```

(Oui, la sous-commande `create-account` vient après les drapeaux du serveur
— elle quitte après avoir créé le compte au lieu de démarrer le serveur. La
politique d'inscription — ouverte, sur invitation ou admin seulement — se
gère ensuite via `admin/update-meta` et les points d'invitation.)

Pour vous rendre admin (ou modérateur) :

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  set-role --username=alice --admin
```

(`--moderator`, et `=false` pour révoquer, fonctionnent de la même façon.)

### Se connecter et publier

pinetree propose plusieurs façons de s'authentifier :

**Via l'API (signin) :**

```bash
curl -s -X POST http://127.0.0.1:3000/api/signin \
  -H 'content-type: application/json' \
  -d '{"username":"alice","password":"a-good-password"}'
```

Cela renvoie `{"i": "your-api-token"}`. Utilisez ce jeton comme `i` dans
toute requête API, ou comme `Authorization: Bearer <token>`.

**Via le web (MiAuth) :** ouvrez `http://127.0.0.1:3000/miauth/<session>`
dans un navigateur après avoir généré un jeton avec
`POST /api/miauth/gen-token`.

**Via un client Misskey :** pointez votre client compatible Misskey préféré
vers l'URL de votre serveur et connectez-vous avec le nom
d'utilisateur/mot de passe ci-dessus.

Puis publiez une note :

```bash
curl -s -X POST http://127.0.0.1:3000/api/notes/create \
  -H 'content-type: application/json' \
  -H 'Authorization: Bearer your-api-token' \
  -d '{"text":"hello from pinetree","visibility":"public"}'
```

---

## Utilisation au quotidien

Une fois lancé, on interagit avec pinetree comme avec n'importe quelle
instance Misskey :

- **Publier :** `POST /api/notes/create` avec `text`, `visibility`
  optionnel, `cw` (avertissement de contenu), `replyId`, `renoteId`,
  `fileIds`, `localOnly`, `poll`. Éditer avec `notes/edit`, voter avec
  `notes/polls/vote`.
- **Fils :** `POST /api/notes/timeline`, `notes/local-timeline`,
  `notes/hybrid-timeline`, `notes/global-timeline` ; vues de fil via
  `notes/replies|renotes|children|conversation` ; `notes/search`,
  `notes/search-by-tag`.
- **Réactions :** `POST /api/notes/reactions/create` avec `noteId` +
  `reaction` (p. ex. `"👍"`) ; omettez `reaction` pour utiliser votre
  `defaultReaction`.
- **Suivis :** `POST /api/following/create` avec `userId` ; les comptes
  verrouillés passent par `following/requests/*`. Blocage/sourdine via
  `blocking/*`, `mute/*`, `renote-mute/*`.
- **Brouillons :** `notes/drafts/create|list|update|delete|count` ; ajoutez
  `scheduledAt` pour publier plus tard.
- **Portabilité :** `i/export-*` / `i/import-*` pour suivis, blocages,
  sourdes, notes, et plus.
- **Streaming :** connectez un WebSocket à `ws://<host>/streaming?i=<token>`
  (ou avec un en-tête Bearer). Les canaux incluent `main`, `homeTimeline`,
  `localTimeline`, `hybridTimeline` et `globalTimeline`.
- **Drive :** `POST /api/drive/files/create` (multipart) pour envoyer ; les
  fichiers sont servis à `GET /files/<id>`.

La surface API complète est documentée dans `docs/endpoint-manifest.md` (le
contrat figé plus les notes de remplacement datées) et
`docs/phase-3-profile.md` (le profil de compatibilité). Tout ce qui ne
figure pas dans cette liste renvoie `404 UNKNOWN_API_ENDPOINT`.

---

## Avec quoi il communique

pinetree fédère en ActivityPub et a été testé contre de vrais logiciels dans
des labos locaux (boucle locale/docker) — jamais contre l'internet public
jusqu'ici :

- **Misskey** (2026.10.1-alpha.0) et **Sharkey** (2025.4.7) : suivis, notes,
  réactions, réponses, citations et flux de livraison dans un labo
  d'interopérabilité en direct à quatre serveurs (deux pinetrees + vrai
  Misskey + vrai Sharkey).
- **Akkoma / famille Pleroma, Mastodon, Catodon et une implémentation de
  référence :** banc d'interopérabilité multi-familles plus large
  (`scripts/fediverse-bench/`) couvrant suivis, interopérabilité des
  fonctions (notes, sondages, réactions, citations) et essais de charge.
  Limites connues issues de ces essais : les réactions émoji sortantes vers
  les hôtes de la famille Akkoma se dégradent en favoris ; les émojis
  personnalisés étrangers ne s'affichent jamais.

Il parle les types d'activité centraux (Follow, Accept, Reject, Undo,
Create, Update, Delete, Like, Announce, Question), WebFinger, NodeInfo, les
signatures HTTP et l'autorisation de citation FEP-044f (flux complet entre
pinetrees ; compatible historique avec Misskey/Sharkey).

---

## Organisation du projet

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

Le code est un **monolithe modulaire** : une seule crate, un sens de
dépendance clair (`domain` ← `application` ← adaptateurs). L'API Misskey et
les adaptateurs ActivityPub appellent les *mêmes* opérations applicatives —
pas de logique métier dupliquée.

---

## Le tester vous-même

La barrière de vérification complète est :

```bash
bash scripts/smoke-all.sh
```

Elle lance (dans l'ordre) :

1. `cargo test --offline` — la suite de tests Rust complète. Actuellement
   **553 verts** : 424 tests unitaires, 100 cas de conformité du stockage sur
   redb, et 29 tests d'intégration (streaming sur WebSockets réels,
   fédération entre nœuds en processus, reprise sur crash et tests
   clients `misskey-js` réels).
2. `scripts/smoke-phase1.sh` — flux centraux pilotés par la vraie
   bibliothèque cliente `misskey-js`.
3. `scripts/smoke-phase3.sh` — les points du profil de compatibilité et le
   streaming via `misskey-js`.
4. `scripts/keystone-sweep.sh` — les appels API exacts du frontend Keystone
   (41 vérifications, toutes passantes).
5. `scripts/smoke-phase2.sh` — interopérabilité de fédération en direct à
   quatre serveurs (vrai Misskey + vrai Sharkey + deux instances pinetree).
   Nécessite Node 22 pour le côté Sharkey ; sauté proprement sans lui.

Vous pouvez aussi lancer des morceaux séparément :

```bash
cargo test --offline                          # Rust tests only
PINETREE_BIN=target/release/pinetree bash scripts/smoke-phase1.sh
```

Voir `docs/testing.md` pour le guide complet, y compris comment lancer le
banc d'interopérabilité et le banc multi-familles à la main.

---

## Contribuer et aller plus loin

- `sns-server-proposal.md` — la proposition produit et architecture. Prime
  sur tout désaccord avec ce README.
- `AGENTS.md` — les règles de conduite pour les contributeurs (humains ou
  IA).
- `docs/` — les contrats figés et le wiki vivant.
- `state/logs/` — journaux de travail datés avec de vraies sorties. Lisez la
  dernière entrée pour vous mettre à jour.

Si vous voulez aider, le plus utile est : l'essayer, lancer la suite de
tests et signaler les problèmes quand quelque chose ne marche pas. Le reste
du Batch M (WP7+ : favoris, émojis, libre-service, suppression d'abonné,
aperçu d'URL, extras du drive) puis la phase 4 (multi-nœud) sont les
chantiers ouverts.

---

## Licence

Pas encore déclarée — il n'y a pas encore de fichier `LICENSE`. Ne présumez
de rien sans demander.
