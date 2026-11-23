# Milestone 2 (leader election), objašnjeno

Ovaj dokument objašnjava šta je urađeno u milestone 2, jednostavno i sa primerima. Detaljan dizajn (na engleskom) je u [DESIGN.md](DESIGN.md), sekcija 3. Brojevi linija (`fajl:linija`) važe za stanje koda u trenutku merge-a milestone-a 2.

---

## 1. Šta milestone 2 radi (za nekog ko ne zna Raft)

1. Imamo tri servera (čvora) koji treba da se dogovore ko je **leader**, tj. jedini čvor koji će, od milestone-a 3, primati upise i prosleđivati ih ostalima.
2. Vreme je podeljeno na **term**-ove (1, 2, 3, ...), a u svakom termu može da postoji **najviše jedan leader**, jer svaki čvor daje najviše jedan glas po termu i taj glas zapisuje na disk pre nego što odgovori.
3. Leader svakih 50 ms šalje **heartbeat** („živ sam“); ako follower ne čuje heartbeat 150–300 ms (nasumično, da ne bi svi krenuli istovremeno), pokušava da postane leader.
4. Pre pravih izbora čvor pita ostale „da li biste glasali za mene?“ (**PreVote**), pa čvor koji je bio odsečen ili se upravo restartovao ne može da „sruši“ zdravog leader-a.
5. Kad leader padne, ostala dva čvora za oko 0,25 s izaberu novog, a sistem ostaje ispravan i kad padne manjina čvorova ili se mreža podeli.

---

## 2. Mapa koda

| Klasa | Šta radi | Prima od | Šalje ka |
|---|---|---|---|
| `RaftKvServer` | Ulazna tačka; čita env promenljive i, ako postoji `RAFTKV_PEERS`, sklapa sve Raft delove | `main` | pravi `RaftNode`, `RaftService`, `GrpcRaftTransport`, `ExecutorScheduler` |
| `ClusterConfig` | Parsira `RAFTKV_NODE_ID`, `RAFTKV_PEERS`, `RAFTKV_RAFT_PORT` | `RaftKvServer.main` | `RaftKvServer` |
| `RaftService` | gRPC server strana `Raft` servisa; nema logike, samo prosleđuje | drugi čvorovi (gRPC) | `RaftNode.onRequestVote` / `onAppendEntries` |
| `GrpcRaftTransport` | gRPC klijent strana; po jedan kanal po peer-u, deadline 100 ms po pozivu | `RaftNode` (preko `RaftTransport`) | `RaftService` drugih čvorova |
| `RaftTransport` | Interfejs „pošalji RPC peer-u“ (u testovima ga zamenjuje simulacija) | `RaftNode` | implementacija |
| `RaftNode` | Srce: stanja FOLLOWER → PRE_CANDIDATE → CANDIDATE → LEADER, tajmeri, brojanje glasova | `RaftService`, odgovori iz transporta, tajmeri iz `Scheduler`-a | `VoteHandler`, `PersistentState`, `RaftTransport` |
| `VoteHandler` | Pravila glasanja: da li dati glas (pravi i PreVote) | `RaftNode` | `PersistentState` (upis terma i glasa) |
| `PersistentState` | Na disku čuva `currentTerm` i `votedFor`; fsync + atomični rename | `VoteHandler`, `RaftNode` | disk (`raft-state`) |
| `Scheduler` / `ExecutorScheduler` | Jedna Raft nit + jedini izvor vremena (`schedule`, `nowMillis`) | `RaftNode` | izvršava taskove na Raft niti |
| `RaftConfig` | Tajminzi: election 150–300 ms, heartbeat 50 ms | `RaftKvServer` | `RaftNode` |
| `RaftStatus`, `Role` | Nepromenljiv snimak stanja (uloga, term, leader) za druge niti | `RaftNode` | testovi, logovi |
| `DurableKvStore` | KV skladište iz milestone-a 1; daje `lastIndex` | `RaftNode`/`VoteHandler` (kroz `LongSupplier`) | — |

Test pomoćnici: `FakeScheduler` (lažni sat, bez `Thread.sleep`), `InMemoryTransport` (test sam odgovara umesto peer-ova), `FixedRandom` (tačno poznati timeout-i), `SimulatedCluster` (3 prava `RaftNode`-a u jednom JVM-u + simulirana mreža).

---

## 3. Tri toka korak po korak

### (a) Prvi start klastera do izbora leader-a

Primer: čvorovi `a`, `b`, `c`, svi u termu 0, niko nije glasao.

1. `RaftKvServer.start()` pokreće gRPC server za `Raft` i poziva `RaftNode.start()`, pa čvor postaje FOLLOWER i `resetElectionTimer()` izvuče nasumičan timeout, npr. `a`: 170 ms, `b`: 240 ms, `c`: 290 ms.
2. Na 170 ms `a`-u ističe tajmer: `onElectionTimeout()` → `becomePreCandidate()`. `a` postaje PRE_CANDIDATE, **term ostaje 0**, i šalje `RequestVote{pre_vote=true, term=1}` peer-ovima.
3. `b` i `c` primaju zahtev: `RaftService` → `RaftNode.onRequestVote()` → `handleRequestVote()` vidi `pre_vote` → `VoteHandler.handlePreVote(req, hasActiveLeader())`. Niko nije čuo leader-a, a logovi su jednaki, pa odgovor je „da“. Ništa se ne upisuje na disk.
4. `a` u `onPreVoteResponse()` broji: `a` + `b` = 2 od 3 = većina → `becomeCandidate()`. Tek sada `PersistentState.startElection("a")` jednim fsync-om upiše term 1 i glas za sebe, i šalje pravi `RequestVote{term=1}`.
5. `b` prima pravi zahtev: `VoteHandler.handle()` → term 1 > 0, pa `advanceTerm(1)`; `b` još nije glasao, log OK → `voteFor("a")` (fsync), tek onda odgovor „da“. `RaftNode` resetuje `b`-ov tajmer jer je glas dat.
6. `a` u `onVoteResponse()` ima 2 glasa → `becomeLeader()`: gasi election tajmer, odmah šalje heartbeat (`sendHeartbeats()`), pa svakih 50 ms (`onHeartbeatTimer()`).
7. `b` i `c` primaju heartbeat → `handleAppendEntries()` → zapamte vreme kontakta sa leader-om, resetuju tajmer, `becomeFollower("a")`.

U logu: `node a became PRE_CANDIDATE in term 0`, `node a became CANDIDATE in term 1`, `node a became LEADER in term 1`.

### (b) Leader padne → novi izbori

Primer: `a` je leader u termu 1 i ugasi se (`docker stop raftkv-a`).

1. Heartbeat-ovi prestaju. `b` i `c` i dalje imaju pokrenut election tajmer (resetovao ga je poslednji heartbeat).
2. Npr. `b`-u tajmer istekne prvi (150–300 ms posle poslednjeg heartbeat-a): `becomePreCandidate()` → PreVote `term=2` ka `a` i `c`.
3. Poziv ka `a` pada (`UNAVAILABLE` ili deadline od 100 ms) → `onPreVoteResponse` ga samo ignoriše.
4. `c` u `hasActiveLeader()` vidi da leader-a nije čuo ≥ 150 ms → kaže „da“. `b` ima većinu → `becomeCandidate()` → term 2 → `c` glasa → `b` je LEADER u termu 2. Ukupno oko 0,25 s.
5. Kad se `a` vrati: `PersistentState.open()` učita term 1 i glas, `a` je FOLLOWER u termu 1. Ako mu tajmer istekne pre prvog heartbeat-a, postane PRE_CANDIDATE, ali `b` (leader) uvek kaže „ne“, a `c` je skoro čuo leader-a, pa kaže „ne“: term se **ne** povećava. Prvi heartbeat od `b` (term 2) prebaci `a` u follower terma 2.

### (c) Mreža se podeli 1 + 2

Primer: leader je `a` u termu 3; `c` je odsečen od `a` i `b`.

1. `a` i `b` normalno rade: heartbeat stiže do `b`, ništa se ne menja. Većina (2 od 3) ima leader-a.
2. `c` ne čuje heartbeat → posle 150–300 ms postaje PRE_CANDIDATE i šalje PreVote. Svi pozivi padaju (nema mreže) → nikad ne dobije većinu → na svaki novi timeout nova PreVote runda, **u istom termu 3**.
3. `c` nikad ne pozove `startElection`, pa njegov term ostaje 3. Bez PreVote-a bi došao do npr. terma 15.
4. Kad se mreža spoji: prvi heartbeat od `a` (term 3) stigne do `c` → `becomeFollower("a")`. Niko ne menja term, leader ostaje `a`.
5. Varijanta: ako je odsečen **leader** `a`, onda `b` i `c` posle ~150 ms više ne čuju leader-a, daju PreVote jedan drugom i izaberu novog leader-a u termu 4. Odsečeni `a` i dalje misli da je leader terma 3 (to je dozvoljeno: jedan leader **po termu**). Kad se mreža spoji, odgovor sa termom 4 ga u `stepDownIfNewerTerm()` vrati u follower.

---

## 4. Pet najvažnijih pravila/invarijanti i gde su u kodu

| # | Pravilo | Zašto | Gde |
|---|---|---|---|
| 1 | **Najviše jedan glas po termu, i to na disku pre odgovora.** | Inače čvor posle restarta može da glasa dvaput u istom termu, pa dva leader-a. | `VoteHandler.java:99` (provera „već glasao za drugog“), `VoteHandler.java:106` (`voteFor`), `PersistentState.java:126` (druga linija odbrane), `PersistentState.java:152`–`163` (fsync + atomični rename) |
| 2 | **Kandidat povećava term i glasa za sebe u jednom fsync-u, pre slanja ijednog RequestVote-a.** | Pad između ta dva koraka ne sme da ostavi čvor u novom termu bez zabeleženog glasa. | `PersistentState.java:139` (`startElection`), `RaftNode.java:300` |
| 3 | **Veći term u pravom RPC-u → usvoji ga i postani follower** (i kad je glas odbijen). | Stari leader/kandidat mora da se povuče čim sazna za noviji term. | `VoteHandler.java:93`, `RaftNode.java:458` (`stepDownIfNewerTerm`), `RaftNode.java:248` (`becomeFollower` gasi heartbeat na liniji 253) |
| 4 | **Glas samo kandidatu čiji je log bar onoliko ažuran** (prvo term poslednjeg zapisa, pa index). | Leader ne sme da bude čvor kome fale commit-ovani zapisi (biće bitno u milestone 3). | `VoteHandler.java:141` (`isLogUpToDate`), korišćeno na 102 i u PreVote-u |
| 5 | **PreVote ne menja ništa i kaže „da“ samo kad nema aktivnog leader-a.** | Restartovan ili odsečen čvor ne sme da podigne term i obori zdravog leader-a. | `VoteHandler.java:126`–`132`, `RaftNode.java:188` (PreVote ne dira term/tajmer), `RaftNode.java:233` (`hasActiveLeader`), `RaftNode.java:419` (odgovor na PreVote ne menja term) |

Dodatno: brojanje glasova samo za tekuću rundu (`RaftNode.java:410`), fail-stop kad upis na disk ne uspe (`RaftNode.java:492`), većina = `n/2 + 1` (`RaftNode.java:119`). U testovima se invarijanta „najviše jedan leader po termu“ i „term nikad ne opada“ proverava posle svakog koraka u `SimulatedCluster.java:275`.

---

## 5. Šta testovi dokazuju, a šta NE

**Ukupno:** `./mvnw verify` → 437 testova, 0 grešaka.

**Dokazuju:**
- **Pravila glasanja** (`VoteHandlerTest`, 16): svako pravilo posebno, provera ulaza (prazan `candidate_id`, `term=0`), glas preživljava restart, PreVote ne menja ništa ni na disku.
- **Prelazi stanja jednog čvora** (`RaftNodeTest`, 26), na tačnu milisekundu, lažnim satom. Primeri:
  - 199 ms: još follower, 200 ms: PreVote;
  - glas iz stare runde se ne računa;
  - odbijen glas ne resetuje tajmer;
  - bivši leader ne šalje heartbeat posle povlačenja;
  - fail-stop.
- **Ponašanje klastera** (`ClusterElectionTest`): 7 scenarija × 50 seed-ova (350 pokretanja):
  - start;
  - pad leader-a;
  - restart starog leader-a bez heartbeat-ova;
  - particija 1+2;
  - povratak izolovanog leader-a;
  - split vote;
  - restart glasača.

  Posle **svakog** taska proverava se Election Safety (najviše jedan leader po termu kroz celu istoriju) i da term nikad ne opada.
- **Testovi stvarno hvataju greške:**
  - uklanjanje provere „već glasao za drugog“: 144/300 pada;
  - uklanjanje i zaštite u `PersistentState`: 126/300 pada sa `Two leaders in term 1`;
  - uklanjanje provere „skoro čuo leader-a“: restart scenario pada za 50/50.
- **gRPC sloj** (`GrpcRaftTransportTest`, 7): pravi `RaftNode` iza in-process gRPC-a; deadline kad peer ne odgovara; `UNAVAILABLE` kad je čvor ugašen.
- **Ručno u Docker-u:**
  - tačno jedan leader na startu;
  - novi leader posle `docker stop` za oko 0,25 s;
  - stari leader posle `docker start` ostaje follower, 3 od 3 puta.

**NE dokazuju:**
- **Formalnu ispravnost.** Simulacija pokriva mnogo slučajnih redosleda, ali ne sve (nije model checking kao TLA+).
- **Prave pakete na mreži:** dupliranje, preuređivanje i kašnjenje duže od 10 ms. Simulacija ima samo kašnjenje 1–10 ms i prekide.
- **Ponašanje sa više od 3 čvora** u klaster testovima (unit testovi pokrivaju i 5).
- **Ništa o podacima:** upisi se još ne repliciraju, pa nema testa da „potvrđen upis nikad nije izgubljen“ (milestone 3 i 4).
- **Kvar diska usred upisa** na pravom fajl sistemu (testirano je samo da `PersistentState` odbije oštećen fajl i da `RaftNode` uradi fail-stop).
- **Pomeranje sata.** `ExecutorScheduler` koristi monotoni `System.nanoTime`, ali to nije posebno testirano.

---

## 6. Poznata ograničenja i šta dolazi u milestone 3

**Ograničenja danas:**
- **Upisi se ne repliciraju.** U klasteru svaki čvor služi `Put`/`Get` iz svog lokalnog skladišta, pa ono što upišeš na `a` ne vidiš na `b`.
- **`lastLogTerm` je uvek 0,** jer zapisi u logu još nemaju term. Provera „log bar onoliko ažuran“ zato sada poredi samo broj zapisa.
- **Hladan start traje ~1 s:** prvi RPC-ovi prekorače deadline od 100 ms dok se uspostavljaju veze. PreVote sprečava da te propale runde dižu term.
- **Dva fsync-a umesto jednog** kad čvor u istom zahtevu usvoji veći term i da glas (bezbedno, samo sporije).
- **Fiksan sastav klastera:** nema dodavanja i uklanjanja čvorova.
- **Odsečen leader ne zna da je odsečen** i i dalje sebe smatra leader-om starog terma (bezbedno, ali bi klijent mogao da mu piše). Rešenje je CheckQuorum ili ReadIndex; vidi DESIGN §4.4.

**Milestone 3 (log replication):**
- `LogEntry` dobija term.
- `AppendEntries` dobija `prev_log_index`, `prev_log_term`, `entries`, `leader_commit`.
- Follower briše deo loga koji se ne slaže sa leader-ovim.
- `commitIndex` napreduje kad je zapis na većini čvorova.
- Upis se potvrđuje klijentu tek posle commit-a.
- Follower-i preusmeravaju klijente na leader-a.
- `lastLogTerm` postaje pravi term poslednjeg zapisa.

---

## 7. Osam pitanja za intervju (sa kratkim odgovorima)

1. **Zašto se `votedFor` upisuje na disk pre odgovora?**
   Ako bi bio samo u memoriji, čvor bi glasao za B, restartovao se, zaboravio i glasao za C u istom termu. B i C bi oba imali većinu, pa bi bila dva leader-a. Ovo pokriva test `restartedNodeDoesNotVoteTwiceInTheSameTerm`.

2. **Zašto su election timeout-i nasumični (150–300 ms)?**
   Da čvorovi retko istovremeno postanu kandidati. Kad se to ipak desi (split vote, scenario e), sledeća runda sa novim nasumičnim timeout-ima reši izjednačenje.

3. **Zašto heartbeat (50 ms) mora biti mnogo kraći od minimalnog election timeout-a (150 ms)?**
   Da follower dobije više heartbeat-ova pre nego što mu istekne tajmer. Inače bi pokretao izbore dok je leader zdrav. `RaftConfig` odbija konfiguraciju koja to ne poštuje.

4. **Kako `RaftNode` izbegava lock-ove?**
   Sve promene stanja su taskovi na jednoj niti (`Scheduler`). gRPC niti i odgovori iz transporta samo predaju posao toj niti (`scheduler.execute`). Druge niti čitaju samo nepromenljivi `RaftStatus`.

5. **Kako testiraš tajmere bez `Thread.sleep`?**
   `Scheduler` je i jedini sat. U testovima je to `FakeScheduler`, čije vreme ide napred samo kad test pozove `advance(ms)`. Sa `FixedRandom` test zna svaki timeout tačno: na 199 ms je follower, na 200 ms PreVote.

6. **Šta je PreVote i koji problem je rešio ovde?**
   Pre pravih izbora čvor pita „da li biste glasali za mene?“ bez povećanja terma. Peer-ovi koji čuju leader-a kažu „ne“. U Docker-u je restartovani stari leader, dok mu heartbeat-ovi još nisu stizali, podizao term i obarao novog leader-a, 3 od 3 puta. Sa PreVote-om ostaje follower.

7. **Kako znaš da tvoji klaster testovi zaista hvataju greške?**
   Namerno sam ubacio greške i video da testovi padaju. Bez provere „već glasao“ (i zaštite u `PersistentState`) pada 126/300 sa `Two leaders in term 1`. Bez PreVote provere „skoro čuo leader-a“ restart scenario pada 50/50. Invarijanta se proverava posle svakog koraka, ne samo na kraju.

8. **Zašto odsečeni stari leader i dalje misli da je leader, i da li je to problem?**
   On ne dobija ništa što bi mu reklo za noviji term. To nije narušavanje bezbednosti: on je leader **starog** terma, a novi leader je u novijem. Pošto ne može da dobije potvrdu većine, u milestone-u 3 neće moći da commit-uje upise. Čim se mreža spoji, vidi veći term i povlači se (scenario d).

---

## 8. „Šta da proverim sam“

Sve komande se pokreću iz korena projekta (u Git Bash-u; na Windows-u umesto `./mvnw` može `mvnw.cmd`).

**1. Svi testovi:**
```bash
./mvnw verify
```
Na kraju treba da piše:
```
[INFO] Tests run: 437, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```
(Jedna linija `ERROR ... node n1 could not persist its Raft state and is stopping` je očekivana: to je test za fail-stop.)

**2. Samo klaster simulacija (350 pokretanja, ~20 s):**
```bash
./mvnw test -Dtest=ClusterElectionTest
```
Očekivano: `Tests run: 350, Failures: 0, Errors: 0`.

**3. Da li testovi hvataju grešku** (radi samo na čistom radnom stablu, bez svojih izmena!):
U `src/main/java/io/github/ncitakovic/raftkv/raft/RaftNode.java` na liniji 237 promeni `return heardFromLeader` u `return false && heardFromLeader`, pa pokreni:
```bash
./mvnw test -Dtest=ClusterElectionTest
```
Očekivano: oko 61 pad, među njima `restartedOldLeaderThatHearsNoHeartbeatsDoesNotDisruptTheNewLeader` za svih 50 seed-ova. Vrati izmenu:
```bash
git checkout -- src/main/java/io/github/ncitakovic/raftkv/raft/RaftNode.java
```

**4. Pravi klaster u Docker-u:**
```bash
docker compose -f docker-compose.cluster.yml up --build -d
docker compose -f docker-compose.cluster.yml logs | grep "became LEADER"
```
Očekivano: tačno jedna linija, npr. `node b became LEADER in term 1`.

**5. Ugasi leader-a** (zameni `b` onim koji je leader):
```bash
docker stop raftkv-b
docker compose -f docker-compose.cluster.yml logs | grep became | tail -4
```
Očekivano: novi `became LEADER in term 2` (veći term), oko 0,25 s posle linije `RaftKV stopped`.

**6. Vrati starog leader-a:**
```bash
docker start raftkv-b
sleep 5
docker logs raftkv-b 2>&1 | grep -E "Loaded Raft state|became" | tail -4
docker compose -f docker-compose.cluster.yml logs | grep "became LEADER" | tail -1
```
Očekivano kod `b`: `Loaded Raft state: term 1, votedFor b`, pa `became FOLLOWER in term 1`. Možda se pojavi i kratko `became PRE_CANDIDATE in term 1` (term ostaje 1!), ali **nikad** `became CANDIDATE` ili `became LEADER`. Poslednji `became LEADER` je i dalje novi leader iz koraka 5.

**7. Počisti:**
```bash
docker compose -f docker-compose.cluster.yml down -v
```
