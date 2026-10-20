> यह प्रोजेक्ट तेज़ी से आगे बढ़ रहा है, और यह दस्तावेज़ जल्दी पुराना हो सकता है। प्रोजेक्ट रूट में अंग्रेज़ी README आमतौर पर अधिक अद्यतन होगा।

# pinetree

pinetree Rust में लिखा एक हल्का, सिंगल-बाइनरी **Misskey-संगत सोशल नेटवर्क
सर्वर** है। मौजूदा Misskey क्लाइंट इससे Misskey API के जानबूझकर चुने गए,
कॉन्ट्रैक्ट-परीक्षित सबसेट (plus स्ट्रीमिंग) के ज़रिए बात करते हैं, और यह
Misskey, Sharkey तथा अन्य fediverse सॉफ़्टवेयर के साथ ActivityPub पर
फ़ेडरेट करता है।

इसे जानबूझकर **चलाने में छोटा** रखा गया है: कोई Postgres नहीं, कोई Redis नहीं,
कोई मैसेज ब्रोकर नहीं। एक बाइनरी, एक एम्बेडेड डेटाबेस फ़ाइल (`redb`), लोकल
फ़ाइल स्टोरेज, बस।

pinetree के साथ कोई फ़्रंटएंड नहीं आता। कोई भी Misskey-संगत वेब
क्लाइंट इसकी ओर इंगित करें — उदाहरण के लिए [Keystone](https://github.com/estyseesghosts/keystone)
(जो pinetree को बैकएंड के रूप में लक्षित करता है) या [KissMe](https://github.com/estyseesghosts/kissme)।

> **स्थिति (2026-10-20):** क्लाइंट
> संगतता, फ़ेडरेशन, स्ट्रीमिंग, कोट पोस्ट, ब्रांच्ड रिप्लाई, पोल,
> पोस्ट संपादन, ड्राफ़्ट/शेड्यूलिंग, ब्लॉकिंग/म्यूटिंग, प्राइवेट सूचियों, पिन,
> अधिसूचना समूहीकरण/फ़िल्टरिंग, प्रोफ़ाइल आयात/निर्यात, और मॉडरेशन +
> पंजीकरण टूलींग के पहले बैच तक सिंगल-नोड सर्वर पूरा हो चुका है। मल्टी-नोड
> क्लस्टरिंग (FoundationDB) अभी भी योजनाबद्ध है, बना नहीं है। विवरण नीचे —
> इसमें यह भी शामिल है कि अभी क्या *जुड़ा* नहीं है।

---

## विषय-सूची

- [यह आज क्या करता है](#what-it-does-today)
- [यह क्या नहीं करता](#what-it-does-not-do)
- [ज्ञात कमियाँ और खुरदुरे किनारे](#known-gaps-and-rough-edges)
- [अभी स्थिति क्या है](#where-things-stand)
- [अपना सर्वर चलाएँ](#run-your-own-server)
  - [पूर्वापेक्षाएँ](#prerequisites)
  - [इसे बनाएँ](#build-it)
  - [इसे शुरू करें](#start-it)
  - [अपना पहला खाता बनाएँ](#create-your-first-account)
  - [लॉग इन करें और पोस्ट करें](#log-in-and-post)
- [रोज़मर्रा में इसका उपयोग](#use-it-day-to-day)
- [यह किनसे बात करता है](#what-it-talks-to)
- [प्रोजेक्ट संरचना](#project-layout)
- [स्वयं परीक्षण करें](#testing-it-yourself)
- [योगदान और आगे पढ़ें](#contributing--reading-more)
- [लाइसेंस](#license)

---

## यह आज क्या करता है

इस खंड की हर चीज़ सिंगल-नोड सर्वर पर चलती है और निष्पादित परीक्षणों से
कवर है ([स्वयं परीक्षण करें](#testing-it-yourself) देखें)। लगभग 95
JSON API एंडपॉइंट `POST /api/{endpoint}` पर उत्तर देते हैं; सूची में न होने
वाली किसी भी चीज़ पर `404 UNKNOWN_API_ENDPOINT` मिलता है, और सूचीबद्ध
एंडपॉइंट पर अज्ञात फ़ील्ड पर `400 UNSUPPORTED_FIELD` मिलता है। सटीक अनुबंध
`docs/endpoint-manifest.md` है।

**खाते, प्रमाणीकरण और पंजीकरण:**

- प्रति-टोकन अनुमतियों के साथ साइनअप / साइनइन, Misskey-शैली
  `429` + `retryAfter` के साथ दर सीमाएँ।
- MiAuth वेब फ़्लो (`miauth/gen-token`, `GET|POST /miauth/{session}[/check]`)
  और लेगेसी `auth/session/generate|show|userkey` + `auth/accept`।
- पंजीकरण नीति: खुला साइनअप, केवल-आमंत्रण (`invite/create|list|delete|limit`,
  `admin/invite/create|list`), या व्यवस्थापक-प्रबंधित (`admin/accounts/create`,
  `admin/meta` / `admin/update-meta` के ज़रिए `disableRegistration`)।
- भूमिकाएँ: व्यवस्थापक और मॉडरेटर (`set-role` CLI), उपयोगकर्ताओं पर पैक किए गए
  `isAdmin` / `isModerator`, एक मॉडरेशन लॉग (`admin/show-moderation-logs`)।
- निलंबन (`admin/suspend-user|unsuspend-user`): `USER_SUSPENDED` के साथ साइन-इन
  अस्वीकृत, खुले स्ट्रीम बंद, सीमाबद्ध स्वीप में फ़ॉलो समाप्त।
- पूर्ण प्रोफ़ाइल स्टोर: `i/update` सभी 39 अपस्ट्रीम प्रोफ़ाइल पैरामीटर स्वीकार
  करता है (name, description, location, birthday, avatar/banner, followedMessage,
  languages, म्यूट किए गए शब्द/instance, अधिसूचना प्राप्ति विन्यास, और अधिक),
  साथ ही pinetree के अपने `rejectQuotes` और `defaultReaction` टॉगल।
- स्व-विलोपन (`i/delete-account`) और व्यवस्थापक विलोपन (`admin/delete-account`),
  व्यवस्थापक उपयोगकर्ता दृश्य (`admin/show-user(s)`, `admin/update-user-note`)।

**नोट और टाइमलाइन:**

- निर्माण (पाठ, सामग्री चेतावनी, `public` / `home` / `followers` /
  `specified` दृश्यता, उत्तर, रीनोट, फ़ाइल संलग्नक, `localOnly`),
  दिखाना, हटाना, अनरीनोट।
- मतदान के साथ **पोल** (`notes/polls/vote`), समाप्ति, और
  `Question` के रूप में फ़ेडरेशन (+ `pollVoted` इवेंट)।
- Sharkey `notes/edit` आकार में **पोस्ट संपादन** (उत्तर अपरिवर्तनीय,
  दृश्यता वापस बाध्य, पोल जोड़ने/बदलने पर वोट रीसेट, संस्करण इतिहास),
  साथ ही `edited` अधिसूचनाएँ और फ़ेडरेटेड `Update(Note)`।
- **कोट पोस्ट** (FEP-044f कैनोनिकल + लेगेसी संगतता + अनुरोध/अनुमोदन
  वर्कफ़्लो; कोटर-पक्ष `rejectQuotes` प्रवर्तन, दूरस्थ स्ट्रिप-टू-सादा-नोट
  फ़ॉलबैक) और **ब्रांच्ड उत्तर थ्रेड** जो मूल के देर से आने पर भी अखंड
  रहते हैं (लंबित मूल रिज़ॉल्वर के जोड़ने तक विफल-बंद रहते हैं)।
- कर्सर पेजिनेशन वाली होम / लोकल / हाइब्रिड / ग्लोबल टाइमलाइन,
  `users/notes`, `notes/replies|renotes|children|conversation`,
  `notes/search|search-by-tag` (स्थानीय नोटों पर सबस्ट्रिंग स्कैन),
  `notes/featured`, `users/featured-notes`।
- हर जगह पठन-समय प्रवर्तन: ब्लॉक/म्यूट/निलंबित लेखक,
  instance ब्लॉक, और रीनोट-म्यूट हर टाइमलाइन और नोट पठन पर फ़िल्टर होते हैं।

**प्रतिक्रियाएँ, फ़ॉलो और सामाजिक ग्राफ़:**

- Misskey सामान्यीकरण के साथ प्रतिक्रिया निर्माण/हटाना (नंगे-`❤` फ़ॉलबैक,
  `likeOnly`, लेगेसी मैपिंग); प्रति-उपयोगकर्ता `defaultReaction` ताकि नंगे
  हार्ट-टैप का समाधान सर्वर-पक्ष हो; `notes/reactions` के ज़रिए पूर्ण
  प्रतिक्रिया सूची।
- फ़ॉलो / अनफ़ॉलो, लॉक किए खातों पर फ़ॉलो अनुरोध
  (`following/requests/list|accept|reject|cancel`)।
- ब्लॉकिंग, म्यूटिंग, रीनोट-म्यूटिंग (`blocking/*`, `mute/*`, `renote-mute/*`,
  `users/relation`), फ़ॉलो/प्रतिक्रिया/मतदान/निर्माण पर और हर पठन पर लागू।
- निजी फ़ॉलोअर/फ़ॉलोइंग सूचियाँ (स्व / फ़ॉलोअर / सार्वजनिक गेटिंग के साथ
  `FORBIDDEN` + गिनती मास्किंग) और निजी प्रतिक्रियाएँ (`users/reactions`)।
- पिन (`i/pin|unpin`, सीमा 5, फ़ेडरेटेड) और दस्तावेज़ीकृत "संगत,
  सरलीकृत" फ़ीचर्ड-नोट रैंकिंग (अपस्ट्रीम-समान नहीं)।

**अधिसूचनाएँ, ड्राफ़्ट, आयात/निर्यात:**

- `i/notifications` (कर्सर, `markAsRead`, शामिल/अपवर्जित प्रकार फ़िल्टर,
  निर्माण-समय प्राप्ति-विन्यास गेट, म्यूटिंग/स्वयं ड्रॉप),
  `i/notifications-grouped` (प्रतिक्रिया/रीनोट समूहीकरण, शुद्ध पैक-परत),
  `notifications/mark-all-as-read`।
- ड्राफ़्ट + अनुसूचित पोस्ट (`notes/drafts/create|list|update|delete|count`;
  शेड्यूलिंग = ड्राफ़्ट पर `scheduledAt`, टिकाऊ जॉब द्वारा प्रकाशित के साथ
  `scheduledNotePosted/Failed` अधिसूचनाएँ)।
- प्रोफ़ाइल आयात/निर्यात: 8 निर्यात (`following`, `blocking`, `mute`, `notes`,
  `antennas`, `clips`, `favorites`, `user-lists`) + अपस्ट्रीम
  त्रुटि कोड वाले 5 आयात और एक `exportCompleted` अधिसूचना। (जिन इकाइयों का
  pinetree के पास कोई मॉडल नहीं है — उपयोगकर्ता सूचियाँ, क्लिप — वे खाली मान्य
  फ़ाइलें निर्यात करती हैं और शून्य पंक्तियाँ आयात करती हैं; एंटेना और
  पसंदीदा वास्तविक पंक्तियाँ निर्यात/आयात करते हैं। यह दस्तावेज़ीकृत है,
  मौन नहीं।)

**स्ट्रीमिंग:**

- प्रमाणीकृत WebSocket `/streaming` (`?i=` या Bearer): `main`,
  `homeTimeline`, `localTimeline`, `hybridTimeline`, `globalTimeline`,
  `antenna` (स्वामी-सीमित `params.antennaId`, मिलानों के लिए `note` फ़्रेम)।
- नोट कैप्चर (`subNote`/`unsubNote` + उपनाम, दृश्यता गेट के साथ
  `reacted`/`unreacted`/`deleted`), `readNotification`, `pollVoted`, `edited`
  नोट अद्यतन, अज्ञात चैनलों का `UNSUPPORTED_CHANNEL` से उत्तर।
- निलंबित उपयोगकर्ताओं के सॉकेट सर्वर-पक्ष से बंद किए जाते हैं।

**मीडिया:**

- मल्टीपार्ट/JSON अपलोड (`drive/files/create`), सूची/दिखाना/अद्यतन/हटाना,
  `GET /files/{id}` पर सार्वजनिक परोसना, स्थानीय फ़ाइलसिस्टम ब्लॉब स्टोर।
- दूरस्थ संलग्नक मान्य और कैश किए जाते हैं (स्कीम, SSRF, आकार सीमा,
  MIME स्निफ़)। कोई फ़ोल्डर नहीं: `folderId` null होना चाहिए।

**फ़ेडरेशन (ActivityPub):**

- WebFinger (JRD + XRD), host-meta, NodeInfo 2.0/2.1, RSA कुंजियों वाले स्थिर
  अभिनेता दस्तावेज़, outbox/followers/following/featured संग्रह, नोट
  दस्तावेज़ (हटाए गए के लिए टॉम्बस्टोन `410`)।
- साझा + व्यक्तिगत इनबॉक्स। डाइजेस्ट + लेगेसी HTTP हस्ताक्षर (`rsa-sha256`)
  आवक सत्यापित और जावक उत्सर्जित; 64 KiB बॉडी सीमा, ±5 मिनट `Date` विचलन,
  अभिनेता/ऑब्जेक्ट प्राधिकार जाँच, परमाणु गतिविधि-ID डुप्लिकेशन-रोक (स्थिति
  परिवर्तन वाले उसी लेनदेन में)।
- गतिविधियाँ: Follow / Accept / Reject / Undo, Create(Note), Update (संपादन और
  पोल वोट), Delete, `_misskey_reaction` वाला Like, Announce, `Question`
  (पोल), pinetree↔pinetree पथ पर FEP-044f कोट प्राधिकरण। आवक
  `EmojiReact` (Akkoma/Pleroma-परिवार) Like पथ से स्वीकार किया जाता है।
- घातांकी बैकऑफ़ के साथ टिकाऊ जावक वितरण, प्रति-गंतव्य सीमाएँ,
  साझा-इनबॉक्स डप-रोक; अंतिम रूप से विफल जॉब हमेशा पुनः प्रयास के बजाय
  लॉग किए गए कारण सहित क्वारंटीन किए जाते हैं।
- instance ब्लॉक (`blockedHosts`): सबडोमेन-जागरूक, आवक पर लागू
  (स्थिति लिखने से पहले छोड़ना) और जावक वितरण समय पर।
- फ़ेडरेशन इनपुट अविश्वसनीय माना जाता है: आकार/समयबाह्य/पुनर्निर्देशन सीमाएँ,
  SSRF सुरक्षा (निजी IP डिफ़ॉल्ट रूप से ब्लॉक), HTML/MFM सैनिटाइज़ेशन।

**टिकाऊपन:**

- हर स्थानीय स्थिति परिवर्तन अपने outbox/जॉब रिकॉर्ड के साथ एक ही
  स्टोरेज लेनदेन में एक साथ कमिट होता है; वर्कर अधिसूचनाएँ, स्ट्रीमिंग
  फ़ैन-आउट और वितरण करते हैं। जॉब निष्क्रिय, डुप्लिकेट-रहित, पुनः
  प्रयास योग्य और क्रैश-सुरक्षित हैं (क्रैश-पुनर्प्राप्ति परीक्षण)।

---

## यह क्या नहीं करता

जानबूझकर गैर-लक्ष्य (प्रस्ताव §1) और स्थगित कार्य — इनके लिए माँगने पर टूटे
आधे-अधूरे कार्यान्वयन के बजाय `404 UNKNOWN_API_ENDPOINT` (API) या
`UNSUPPORTED_CHANNEL` (स्ट्रीमिंग) मिलता है:

- **कोई मल्टी-नोड मोड नहीं।** `--mode=cluster` शुरू होने से इनकार करता है
  (`cluster mode is not implemented (Phase 4)`)। एक प्रक्रिया, एक एम्बेडेड
  `redb` फ़ाइल, कोई FoundationDB एडाप्टर नहीं, कोई खाता निर्देशिका नहीं, कोई
  क्रॉस-नोड रूटिंग नहीं। वह Phase 4 है, शुरू नहीं हुआ।
- **कोई बंडल वेब क्लाइंट नहीं।** Keystone, KissMe, या कोई Misskey-संगत
  क्लाइंट उपयोग करें।
- **कोई Mastodon क्लाइंट REST API नहीं।** Mastodon-परिवार सर्वरों *के
  साथ* ActivityPub फ़ेडरेशन दायरे में रहता है; Mastodon API स्वयं एक
  गैर-लक्ष्य है (Sharkey एक शिप करता है; प्रस्ताव बदले बिना pinetree कभी
  नहीं करेगा)।
- **कस्टम इमोजी instance-क्यूरेटेड हैं।** सार्वजनिक `emojis`/`emoji` सूची,
  `admin/emoji/add|update|delete|list` (मॉडरेटर), केस-असंवेदनशील विशिष्टता
  वाली शॉर्टकोड रजिस्ट्री, प्रति-उपयोगकर्ता दूरस्थ-इमोजी ऑप्ट-इन
  (`showRemoteEmojis`, मेटाडेटा अंतर्ग्रहण + आलसी फ़ेच)। विदेशी `:shortcode:`
  प्रतिक्रियाएँ शब्दशः संग्रहित होती हैं। Akkoma/Pleroma-परिवार होस्टों पर
  जावक प्रतिक्रियाएँ सादे `Like` (❤ पसंदीदा) पर अवनत होती हैं जब तक कोई
  `EmojiReact` उत्सर्जन आकार लाइव राउंड-ट्रिप परीक्षण पार न करे।
- **उपयोगकर्ता सूचियाँ शिप हो गईं (Batch O WP16)।** `users/lists/*` (12
  एंडपॉइंट), `notes/user-list-timeline` (केवल-स्वामी पुल पठन), `userList`
  स्ट्रीमिंग चैनल (`userAdded`/`userRemoved`/नोट), सूची अधिसूचना
  प्राप्ति-विन्यास, CSV निर्यात/आयात, और एंटेना `src=list` अब सदस्यता सूचकांक
  से हल होता है। सीमाएँ लागू + विज्ञापित (`userListLimit 10`,
  `userEachUserListsLimit 50`)। दूरस्थ सदस्य सिस्टम-अभिनेता Follow कतारबद्ध
  करते हैं। `create-from-public` 404 रहता है।
- **कोई क्लिप, चैनल, पेज, ड्राइव फ़ोल्डर, चैट,
  Reversi, या हैशटैग-द्वारा-खोज नहीं।** कोई फ़ीचर्ड टाइमलाइन नहीं,
  कोई `drive` स्ट्रीमिंग चैनल नहीं,
  कोई `signin-flow` नहीं, कोई 2FA/पासकी/वेबहुक नहीं।
  एंटेना, पसंदीदा, कस्टम इमोजी, उपयोगकर्ता सूचियाँ, मेंशन टाइमलाइन,
  अधिसूचना उपयोगिताएँ, डिस्कवरी, संस्करण, और अनुवाद स्टब शिप हो गए
  (Batch N + O + P)।
- **कोई OAuth नहीं।** केवल MiAuth + API टोकन (OAuth तभी आएगा जब किसी लक्षित
  क्लाइंट को इसकी आवश्यकता हो)।
- **कोई थ्रेड म्यूटिंग नहीं, कोई RFC 9421 या लिंक्ड-डेटा
  हस्ताक्षर नहीं,** कोई `_misskey_content` नहीं (निर्णय से कभी उत्सर्जित नहीं),
  दस्तावेज़ीकृत त्रुटियों से परे हटाए गए/समाप्त किनारे पर कोई पोल नहीं। खाता
  स्थानांतरण (`i/move` + आवक `Move`) Batch P में शिप करता है — दो pinetree के
  बीच लाइव सत्यापित।
- **स्व-सेवा, फ़ॉलोअर-हटाना, दूरस्थ लुकअप, ड्राइव अतिरिक्त, और
  url-preview शिप हो गए (Batch O WP9/WP10/WP12/WP13)।**
  `i/change-password`, `i/regenerate-token` (मूल रोटेशन; ऐप अनुदान
  बचे रहते हैं), `i/apps`, `i/authorized-apps`, वास्तविक
  `i/revoke-token{tokenId}`, `i/signin-history` (परिनियोजन-अग्रेषित पंक्तियाँ,
  100/उपयोगकर्ता पर सीमित), `following/invalidate` (दूरस्थ फ़ॉलोअर को
  फ़ॉलोई-लिखित Reject), `users/search-by-username-and-host`, `ap/get`
  (`ap/show` का क्रेडेंशियल वाला सुरक्षित सबसेट), `drive/files/upload-from-url`,
  `drive/files/find-by-hash`, प्रथम-पक्ष `url-preview`।
- **ईमेल पहचान + डिस्कवरी बैच शिप हो गया (Batch P)।**
  `i/update-email`, `request-reset-password`, `reset-password`,
  `verify-email` (कोड-पूर्ण, निष्क्रिय: कोई SMTP विन्यस्त नहीं — भेजना ज़ोर से
  विफल होता है, कभी चुपचाप नहीं), `i/move` + आवक `Move` (उपनाम हैंडशेक,
  फ़ॉलोअर प्रवासन, पुनरावृत्ति/जालसाज़ी कवर), `i/registry/*` (8 एंडपॉइंट),
  `get-online-users-count` (स्ट्रीमिंग-उपस्थिति परिभाषा), `pinned-users`
  (`[]`), `fetch-external-resources`, `fetch-rss` (दस्तावेज़ीकृत सबसेट),
  `server-info`, `stats` (वास्तविक गिनती), `notes/mentions`,
  `notifications/test-notification`, `notifications/flush` (मार्क-रीड +
  इवेंट), `users/recommendation`, `notes/recommendations` (सरलीकृत
  स्कोरिंग), `notes/versions` (Sharkey आकार), `notes/translate`
  (निष्क्रिय: कोई बैकएंड नहीं — ज़ोरदार विफलता)।
- **दुरुपयोग रिपोर्टिंग जुड़ी हुई है** (`users/report-abuse` + `Flag`
  अग्रेषण वाला `admin/abuse-user-reports` परिवार); व्यवस्थापक सूची का
  सर्वेक्षण करते हैं (कोई मेल/वेबहुक अधिसूचना नहीं)।

---

## ज्ञात कमियाँ और खुरदुरे किनारे

चीज़ें काम करती हैं लेकिन दस्तावेज़ीकृत सीमाओं के साथ जिनसे आप टकरा सकते हैं:

- **केवल लूपबैक-परीक्षित।** इंटरऑप स्थानीय प्रयोगशालाओं में वास्तविक सर्वरों
  के विरुद्ध सत्यापित है (नीचे देखें), लाइव सार्वजनिक इंटरनेट पर कभी नहीं।
  SSRF सुरक्षा डिफ़ॉल्ट रूप से निजी IP ब्लॉक करती है; लूपबैक फ़ेडरेशन के लिए
  `PINETREE_ALLOW_PRIVATE_IPS=1` चाहिए।
- **सिंगल-नोड स्टोरेज।** एक लेखक (`redb`), कोई पठन प्रतिकृति नहीं, मल्टी-नोड
  पर कोई लाइव प्रवासन पथ नहीं (निर्यात/आयात खातों, फ़ॉलो,
  ब्लॉक, म्यूट और नोटों को कवर करता है — पारदर्शी उन्नयन नहीं)।
- **खोज कैश किए नोटों पर सीमाबद्ध सबस्ट्रिंग स्कैन है**, सूचकांक नहीं।
  `notes/reactions` केवल-`sinceId` कर्सर पर नवीनतम-पहले पेज करता है
  (Misskey के आरोही क्रम से दस्तावेज़ीकृत विचलन)।
- **फ़ीचर्ड नोट सरलीकृत रैंकिंग उपयोग करते हैं** (प्रतिक्रियाओं + रीनोट से
  स्कोर किए गए हालिया गैर-संवेदनशील सार्वजनिक नोट) — आकार में संगत,
  अपस्ट्रीम-समान नहीं।
- **अभिनेता RSA कुंजियाँ पहले उपयोग पर आलसी उत्पन्न होती हैं** — डीबग बिल्ड
  में धीमी, इसलिए नए खाते की पहली दूरस्थ फ़ेच सहकर्मियों के लिए टाइमआउट हो
  सकती है।
- **शब्द म्यूट सर्वर-प्रवर्तित हैं** (अपस्ट्रीम से स्वामी-अनुमोदित विचलन, जो
  उन्हें केवल क्लाइंट-पक्ष लागू करता है): `hardMutedWords` हर पठन पर नोट
  छोड़ते हैं; `mutedWords` दर्शक-विशिष्ट `isSoftMuted` फ़्लैग पैक करते हैं
  (केवल मिलने पर सत्य और नोट का अपना CW न होने पर)। `docs/word-mutes.md`
  देखें।
- **निजी फ़ॉलो सूचियों में अभी प्रति-उपयोगकर्ता दृश्यता नियंत्रण नहीं हैं** —
  प्रोफ़ाइल फ़ील्ड संग्रहित करती है, सूचियाँ दस्तावेज़ीकृत नियम पर गेट करती
  हैं (स्व/फ़ॉलोअर/सार्वजनिक)।
- **कोई प्रति-गंतव्य वितरण ट्यूनिंग नहीं, प्रतिक्रिया-दर-नोट सूचकांक से परे
  कोई टाइमलाइन सूचकांक नहीं, न्यूनतम अवलोकनीयता** (केवल stderr निदान)।
  वह Phase 5 का कार्य है और केवल मापों से शुरू होता है।
- **बेंचमार्क डीबग-बिल्ड, सिंगल-नोड संख्याएँ हैं** (~870 नोट-निर्माण/से,
  ~430 टाइमलाइन/से, ~1200 उपयोगकर्ता-दिखाना/से) — मोटा मार्गदर्शन, वादा नहीं।

---

## अभी स्थिति क्या है

| चरण | वह क्या था | स्थिति |
|---|---|---|
| 0 | अनुबंध फ़्रीज़ करना (API मेनिफ़ेस्ट, पहचान नियम, स्टोरेज परीक्षण) | पूर्ण |
| 1 | सिंगल-नोड सर्वर (खाते, नोट, फ़ॉलो, प्रतिक्रियाएँ, ड्राइव, जॉब) | पूर्ण |
| 2 | फ़ेडरेशन (WebFinger, अभिनेता, हस्ताक्षर, इनबॉक्स, वितरण, डप-रोक) | पूर्ण |
| 3 | क्लाइंट संगतता और स्ट्रीमिंग (लाइव WebSocket, समृद्ध API, वास्तविक-क्लाइंट अनुबंध परीक्षण) | पूर्ण |
| — | ऊपर से कोट पोस्ट (FEP-044f) और ब्रांच्ड उत्तर | पूर्ण |
| — | Misskey समता बैच: पोल, संपादन, ड्राफ़्ट/शेड्यूलिंग, ब्लॉक/म्यूट, निजी सूचियाँ, पिन, अधिसूचना समूहीकरण/फ़िल्टरिंग, आयात/निर्यात | पूर्ण |
| — | मॉडरेशन बैच (Batch M, Phase 4 से पूर्व): भूमिकाएँ, पंजीकरण/आमंत्रण, निलंबन, instance ब्लॉक, व्यवस्थापक दृश्य; API परत के नीचे दुरुपयोग-रिपोर्ट प्लंबिंग | अधिकतर पूर्ण — WP1–WP6 उतरे, WP7+ लंबित |
| — | दैनिक-उपयोग बैच (Batch N): पसंदीदा + नोट स्थिति, कस्टम इमोजी (+ दूरस्थ ऑप्ट-इन), `followRequestAccepted`, एंटेना, दुरुपयोग-रिपोर्ट मार्ग, व्यवस्थापक शेष, शब्द म्यूट (सर्वर-पक्ष), MiAuth कठोरीकरण, Sharkey शेड्यूल शिम | पूर्ण (इस सत्र में) — `url-preview`, स्व-सेवा, फ़ॉलोअर हटाना, उपयोगकर्ता सूचियाँ अभी लंबित |
| 4 | FoundationDB पर मल्टी-नोड | योजनाबद्ध — शुरू नहीं हुआ |
| 5 | स्केल और कठोरीकरण | योजनाबद्ध |

सिंगल-नोड सर्वर **आज चलाने के लिए तैयार** है। Phase 4 आने तक मल्टी-नोड मोड
शुरू होने से इनकार करता है (यह जानबूझकर है)।

---

## अपना सर्वर चलाएँ

### पूर्वापेक्षाएँ

1. **Rust** (हालिया स्थिर)। यदि आपके पास नहीं है तो [rustup](https://rustup.rs/) से
   लें।
2. सर्वर स्वयं चलाने के लिए बस इतना ही।

> यदि आप बाद में पूर्ण परीक्षण सूट या इंटरऑप प्रयोगशालाएँ चलाना चाहते हैं, तो
> आपको `bun`, Docker (compose प्लगइन सहित), Node 24, और Node 22 भी चाहिए होंगे।
> लेकिन *pinetree चलाने* के लिए आपको केवल Rust चाहिए।

### इसे बनाएँ

```bash
cargo build --release
```

यह `target/release/pinetree` पर एकल बाइनरी बनाता है।

> **यदि आप WSL पर हैं:** अपना चेकआउट मूल Linux फ़ाइलसिस्टम पर रखें
> (जैसे `/home/you/pinetree`, `/mnt/c/...` पर नहीं)। 9P फ़ाइलसिस्टम बिल्ड और
> डेटाबेस के लिए कई गुना धीमा है। यदि आप बिल्ड
> आर्टिफ़ैक्ट तेज़ ड्राइव पर चाहते हैं तो `.cargo/config.toml`
> (git-उपेक्षित) में कस्टम लक्ष्य डायरेक्टरी भी सेट कर सकते हैं।

### इसे शुरू करें

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  --listen=127.0.0.1:3000
```

- `--public-url` वह URL है जिसे दुनिया (और अन्य सर्वर) देखेगी। यह
  `http://` या `https://` से शुरू होना चाहिए और होस्ट शामिल करना चाहिए।
  अभिनेता URL, नोट URL और WebFinger सभी इसी से बनाए जाएँगे।
- `--data-dir` वह जगह है जहाँ एम्बेडेड डेटाबेस (`pinetree.redb`) और कोई भी
  अपलोड की गई फ़ाइलें रहती हैं। इसे ऐसी डायरेक्टरी की ओर इंगित करें जिसका
  बैकअप ले सकें।
- `--listen` वह पता है जिससे HTTP सर्वर बंधता है। इसे स्थानीय-केवल रखने के लिए
  `127.0.0.1:3000` उपयोग करें, या अपने नेटवर्क पर उजागर करने के लिए
  `0.0.0.0:3000`।

यदि आप इसे स्थानीय रूप से आज़मा रहे हैं, तो यह भी काम करता है:

```bash
target/release/pinetree \
  --public-url=http://127.0.0.1:3000 \
  --data-dir=./data \
  --listen=127.0.0.1:3000
```

> डिफ़ॉल्ट रूप से, निजी/आंतरिक IP पर फ़ेडरेशन अनुरोध ब्लॉक किए जाते हैं (SSRF
> सुरक्षा)। आपकी मशीन पर सर्वरों के बीच केवल-स्थानीय फ़ेडरेशन परीक्षण के लिए,
> आप `PINETREE_ALLOW_PRIVATE_IPS=1` से ऑप्ट इन कर सकते हैं। उत्पादन में
> आप ब्लॉक चालू रखना चाहते हैं।

### अपना पहला खाता बनाएँ

आप कमांड लाइन से खाते बना सकते हैं:

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  create-account --username=alice --password=a-good-password
```

(हाँ, `create-account` उपकमांड सर्वर फ़्लैग के बाद आता है — यह सर्वर शुरू करने
के बजाय खाता बनाकर बाहर निकलता है। पंजीकरण नीति
— खुली, केवल-आमंत्रण, या केवल-व्यवस्थापक — बाद में
`admin/update-meta` और आमंत्रण एंडपॉइंट के ज़रिए प्रबंधित होती है।)

स्वयं को व्यवस्थापक (या मॉडरेटर) बनाने के लिए:

```bash
target/release/pinetree \
  --public-url=https://social.example.com \
  --data-dir=./data \
  set-role --username=alice --admin
```

(`--moderator`, और हटाने के लिए `=false`, उसी तरह काम करते हैं।)

### लॉग इन करें और पोस्ट करें

pinetree प्रमाणीकरण के कुछ तरीके समर्थित करता है:

**API के ज़रिए (signin):**

```bash
curl -s -X POST http://127.0.0.1:3000/api/signin \
  -H 'content-type: application/json' \
  -d '{"username":"alice","password":"a-good-password"}'
```

यह `{"i": "your-api-token"}` लौटाता है। उस टोकन को किसी भी API
अनुरोध में `i` के रूप में, या `Authorization: Bearer <token>` के रूप में उपयोग
करें।

**वेब के ज़रिए (MiAuth):** `POST /api/miauth/gen-token` से टोकन बनाने के बाद
ब्राउज़र में `http://127.0.0.1:3000/miauth/<session>` खोलें।

**Misskey क्लाइंट के ज़रिए:** अपना पसंदीदा Misskey-संगत क्लाइंट अपने सर्वर के
URL की ओर इंगित करें और उपरोक्त उपयोगकर्ता नाम/पासवर्ड से लॉग इन करें।

फिर एक नोट पोस्ट करें:

```bash
curl -s -X POST http://127.0.0.1:3000/api/notes/create \
  -H 'content-type: application/json' \
  -H 'Authorization: Bearer your-api-token' \
  -d '{"text":"hello from pinetree","visibility":"public"}'
```

---

## रोज़मर्रा में इसका उपयोग

एक बार चलने पर, आप pinetree से वैसे ही इंटरैक्ट करते हैं जैसे किसी Misskey
instance से करते:

- **पोस्ट:** `text`, वैकल्पिक `visibility`, `cw` (सामग्री चेतावनी), `replyId`,
  `renoteId`, `fileIds`, `localOnly`, `poll` के साथ `POST /api/notes/create`।
  `notes/edit` से संपादित करें, `notes/polls/vote` से मतदान करें।
- **टाइमलाइन:** `POST /api/notes/timeline`, `notes/local-timeline`,
  `notes/hybrid-timeline`, `notes/global-timeline`; थ्रेड दृश्य
  `notes/replies|renotes|children|conversation` से; `notes/search`,
  `notes/search-by-tag`।
- **प्रतिक्रियाएँ:** `noteId` + `reaction` के साथ `POST /api/notes/reactions/create`
  (जैसे `"👍"`); अपने `defaultReaction` का उपयोग करने के लिए `reaction` छोड़ दें।
- **फ़ॉलो:** `userId` के साथ `POST /api/following/create`; लॉक किए खाते
  `following/requests/*` से जाते हैं। `blocking/*`, `mute/*`,
  `renote-mute/*` से ब्लॉक/म्यूट करें।
- **ड्राफ़्ट:** `notes/drafts/create|list|update|delete|count`; बाद में
  प्रकाशित करने के लिए `scheduledAt` जोड़ें।
- **पोर्टेबिलिटी:** फ़ॉलो, ब्लॉक, म्यूट, नोट और अधिक के लिए `i/export-*` /
  `i/import-*`।
- **स्ट्रीमिंग:** `ws://<host>/streaming?i=<token>` से WebSocket जोड़ें (या
  Bearer हेडर के साथ)। चैनलों में `main`, `homeTimeline`,
  `localTimeline`, `hybridTimeline`, और `globalTimeline` शामिल हैं।
- **ड्राइव:** अपलोड करने के लिए `POST /api/drive/files/create` (मल्टीपार्ट);
  फ़ाइलें `GET /files/<id>` पर परोसी जाती हैं।

पूर्ण API सतह `docs/endpoint-manifest.md` (फ़्रीज़ अनुबंध प्लस दिनांकित
अधिक्रमण नोट) और `docs/phase-3-profile.md` (संगतता प्रोफ़ाइल) में
दस्तावेज़ीकृत है। उस सूची में न होने वाली किसी भी चीज़ पर
`404 UNKNOWN_API_ENDPOINT` मिलता है।

---

## यह किनसे बात करता है

pinetree ActivityPub पर फ़ेडरेट करता है और स्थानीय (लूपबैक/docker)
प्रयोगशालाओं में वास्तविक सॉफ़्टवेयर के विरुद्ध परीक्षित है — अब तक सार्वजनिक
इंटरनेट पर कभी नहीं:

- **Misskey** (2026.10.1-alpha.0) और **Sharkey** (2025.4.7): लाइव चार-सर्वर
  इंटरऑप प्रयोगशाला (दो pinetree + वास्तविक Misskey + वास्तविक Sharkey) में
  फ़ॉलो, पोस्ट, प्रतिक्रिया, उत्तर और वितरण फ़्लो।
- **Akkoma / Pleroma-परिवार, Mastodon, Catodon, और एक संदर्भ कार्यान्वयन:**
  फ़ॉलो, फ़ीचर इंटरऑप (नोट, पोल, प्रतिक्रियाएँ, कोट), और स्ट्रेस रन कवर करने
  वाला व्यापक बहु-परिवार बेंच हार्नेस (`scripts/fediverse-bench/`)।
  उन रनों की ज्ञात स्थायी सीमाएँ: Akkoma-परिवार होस्टों पर जावक इमोजी
  प्रतिक्रियाएँ पसंदीदा पर अवनत होती हैं; विदेशी कस्टम इमोजी कभी
  रेंडर नहीं होता।

यह मुख्य गतिविधि प्रकार (Follow, Accept, Reject, Undo, Create,
Update, Delete, Like, Announce, Question), WebFinger, NodeInfo, HTTP
हस्ताक्षर, और FEP-044f कोट प्राधिकरण बोलता है (pinetree के बीच पूर्ण
वर्कफ़्लो; Misskey/Sharkey के साथ लेगेसी-संगत)।

---

## प्रोजेक्ट संरचना

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

कोड एक **मॉड्यूलर मोनोलिथ** है: एक क्रेट, स्पष्ट निर्भरता दिशा
(`domain` ← `application` ← एडाप्टर)। Misskey API और ActivityPub
एडाप्टर *समान* एप्लिकेशन संचालन बुलाते हैं — कोई दोहरा व्यावसायिक तर्क
नहीं।

---

## स्वयं परीक्षण करें

पूर्ण सत्यापन गेट है:

```bash
bash scripts/smoke-all.sh
```

वह क्रम से चलाता है (क्रम में):

1. `cargo test --offline` — पूर्ण Rust परीक्षण सूट। वर्तमान में **553 हरे**:
   424 इकाई परीक्षण, redb पर 100 स्टोरेज-अनुरूपता मामले, और 29 एकीकरण
   परीक्षण (लाइव WebSocket पर स्ट्रीमिंग, इन-प्रोसेस नोडों के बीच फ़ेडरेशन,
   क्रैश पुनर्प्राप्ति, और वास्तविक-`misskey-js` क्लाइंट धुआँ परीक्षण)।
2. `scripts/smoke-phase1.sh` — वास्तविक `misskey-js` क्लाइंट लाइब्रेरी द्वारा
   संचालित मुख्य फ़्लो।
3. `scripts/smoke-phase3.sh` — `misskey-js` के ज़रिए संगतता प्रोफ़ाइल
   एंडपॉइंट और स्ट्रीमिंग।
4. `scripts/keystone-sweep.sh` — Keystone फ़्रंटएंड के सटीक API कॉल (41
   जाँच, सभी उत्तीर्ण)।
5. `scripts/smoke-phase2.sh` — लाइव चार-सर्वर फ़ेडरेशन इंटरऑप (वास्तविक
   Misskey + वास्तविक Sharkey + दो pinetree instance)। Sharkey पक्ष के लिए
   Node 22 चाहिए; इसके बिना साफ़ छोड़ देता है।

आप टुकड़े अलग-अलग भी चला सकते हैं:

```bash
cargo test --offline                          # Rust tests only
PINETREE_BIN=target/release/pinetree bash scripts/smoke-phase1.sh
```

इंटरऑप रिग और बहु-परिवार बेंच हार्नेस को हाथ से चलाने सहित पूर्ण मार्गदर्शन
के लिए `docs/testing.md` देखें।

---

## योगदान और आगे पढ़ें

- `sns-server-proposal.md` — उत्पाद और वास्तुकला प्रस्ताव। इस README से किसी
  भी असहमति पर यही मान्य होगा।
- `AGENTS.md` — योगदानकर्ताओं (मानव या AI) के लिए मार्ग के नियम।
- `docs/` — फ़्रीज़ अनुबंध और जीवित विकी।
- `state/logs/` — वास्तविक आउटपुट वाले दिनांकित कार्य लॉग। गति पकड़ने के लिए
  नवीनतम प्रविष्टि पढ़ें।

यदि आप मदद करना चाहते हैं, तो सबसे उपयोगी चीज़ें हैं: इसे आज़माना, परीक्षण
सूट चलाना, और कुछ काम न करे तो समस्या दर्ज करना। Batch M शेष (WP7+:
पसंदीदा, इमोजी, स्व-सेवा, फ़ॉलोअर हटाना, URL पूर्वावलोकन, ड्राइव अतिरिक्त)
और फिर Phase 4 (मल्टी-नोड) खुले मोर्चे हैं।

---

## लाइसेंस

अभी घोषित नहीं — कोई `LICENSE` फ़ाइल नहीं है। मानने से पहले पूछें।
