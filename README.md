# TP ZooKeeper-like - Yuefan Liu

Java 17, Maven et gRPC. Cinq processus independants : un leader, quatre followers.

## Lien avec les TP precedents

Le TP gRPC Calculator utilisait `ServerBuilder`, `ManagedChannelBuilder`, un
blocking stub et `StreamObserver`. On garde exactement ces mecanismes : les RPC
Add/Divide sont remplaces par Write/Guess/Read et les RPC entre noeuds. Le menu
`Scanner` reprend le principe du SocketClient. Les classes generees par protoc
ne sont pas a modifier. Les anciens projets restent inchanges.

## Compiler

Dans PowerShell, depuis la racine du projet, avec Java 17+ :

```powershell
Set-Location 'D:\zooKeeperLab'
./scripts/build.ps1
```

Les scripts trouvent Maven dans PATH ou dans IntelliJ IDEA. Si PowerShell bloque
les scripts, utiliser pour cette session uniquement :

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```

Alternative avec Maven dans PATH :

```powershell
mvn compile
mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 1"
mvn exec:java "-Dexec.mainClass=fr.isep.client.ClientApp" "-Dexec.args=--node 4 --client A"
```

## Lancer les cinq noeuds

Ouvrir cinq terminaux dans le projet, puis executer une ligne par terminal :

```powershell
./scripts/run-node.ps1 -Id 1
./scripts/run-node.ps1 -Id 2
./scripts/run-node.ps1 -Id 3
./scripts/run-node.ps1 -Id 4
./scripts/run-node.ps1 -Id 5
```

Ports : 5001 a 5005. Journaux : `data/node-1/entries.log` a
`data/node-5/entries.log`. Attendre `LEADER ready` et les messages `SYNC`.
Le quorum reste toujours 3/5, meme si certains noeuds sont arretes.
Sur un dossier de donnees neuf, les tags sont egaux et le noeud 1 gagne.
Apres d'autres executions, le gagnant depend des journaux persistants.

Options : `-BasePort 6000` (tous les noeuds et clients doivent utiliser cette
valeur), `-DataDir demo-data` (pour isoler une nouvelle demonstration).
Ne jamais effacer les journaux pendant une demonstration de reprise.

Les scripts utilisent un repertoire inexistant pour le selector interne de
Windows JDK 17. Cela contourne une erreur de socket Unix constatee sur cette
machine. Ce repertoire ne doit pas etre cree. Les RPC restent sur TCP/gRPC.

Dans IntelliJ : ouvrir pom.xml comme projet Maven, generer avec `compile`,
puis creer cinq configurations `fr.isep.node.NodeServer`, arguments `--id 1` a
`--id 5`. Working directory : racine du projet. Si necessaire, ajouter l'option VM
`-Djdk.net.unixdomain.tmpdir=D:/zooKeeperLab/target/nonexistent-socket-dir`.
Le client est `fr.isep.client.ClientApp`, arguments `--node 4 --client A`.

## Demonstration 1 : Write

Dans un sixieme terminal :

```powershell
./scripts/run-client.ps1 -Node 4 -Client A
```

Saisir `write 50`, puis `read`. Le follower 4 transmet au leader.
Dans le terminal leader : PROPOSE, ACK, COMMIT. Le leader compte sa propre
copie durable et attend au moins deux ACK de followers. Les cinq noeuds rendent
la cible 50 apres reception du COMMIT.
Pour lire chaque noeud sans ouvrir un client interactif :

```powershell
1..5 | ForEach-Object { ./scripts/run-client.ps1 -Node $_ -Client inspect -Command 'read' }
```

`REQUEST_COMMITTED` signifie une operation commise. `REQUEST_ACCEPTED` signifie
une guess mise en attente. `REQUEST_REJECTED` signifie un refus.
`REQUEST_UNCERTAIN` signifie que le resultat final doit etre verifie par lecture :
un proposal peut etre termine lors de la reprise. Le programme n'effectue pas
une nouvelle ecriture automatiquement. Les IDs de requete sont affiches et les
RPC dedupliquent les retries de meme client/ID ; le menu cree un nouvel ID a
chaque commande.

## Demonstration 2 : Guess

Ouvrir deux autres terminaux avant de commencer :

```powershell
./scripts/run-client.ps1 -Node 3 -Client B
./scripts/run-client.ps1 -Node 5 -Client C
```

Avec target=50, saisir `guess 30` chez B et `guess 62` chez C dans les trois
secondes suivant la premiere guess. Attendre trois secondes, puis `read`.
Le leader affiche WINNER value=62, execute PROPOSE/ACK/COMMIT et demarre la ronde
suivante avec target=62. Distance : |30-50|=20, |62-50|=12.

Choix du jeu : une fenetre de trois secondes demarre a la premiere guess ; une
guess par client/ronde ; egalite resolue par l'ordre d'arrivee. Le client lit la
ronde avant de soumettre. Une guess de ronde obsolete est refusee. Les guesses
en memoire qui n'ont pas encore produit de proposal sont perdues en cas de crash
et doivent etre soumises de nouveau. Write peut aussi changer la cible pour
preparer un autre exemple et annule les guesses en attente de l'ancienne ronde.

## Demonstration 3 : Crash du leader

Lire `leader=...`, puis fermer le terminal de CE noeud ou Ctrl+C dans ce terminal.
Attendre environ 6 a 10 secondes. Les followers detectent l'absence de heartbeat.
Ils comparent le dernier tag, y compris PROPOSED : plus grand epoch, puis seq,
puis plus petit ID en cas d'egalite. Le gagnant rassemble au moins trois journaux,
preserve les commits, synchronise la queue et termine les proposals selectionnes
apres nouvelle replication a quorum. Il persiste le nouvel epoch et remet seq a 0.
Les clients connectes a un follower vivant peuvent refaire `read`, puis `write 40`.
Un client connecte au noeud tue doit etre relance sur un noeud vivant.
Relancer le noeud arrete avec le meme Id et DataDir : il reprend comme follower
et rattrape le journal du nouveau leader.

## Architecture et choix

| Classe | Role |
| --- | --- |
| NodeServer | Demarrage gRPC et arret du processus |
| NodeRuntime | Connexions reutilisees et assemblage des composants |
| GameServiceImpl | Write/Guess/Read, comme CalculatorServiceImpl |
| CoordinationServiceImpl | RPC PROPOSE, COMMIT, heartbeat, election, synchronisation |
| LeaderCoordinator | Numero de transaction, ACK durables, quorum, COMMIT |
| ElectionManager | Heartbeat 500 ms, timeout 5 s + decalage par ID, reprise |
| GameEngine | Collecte trois secondes, distance absolue, nouvelle ronde |
| NodeState | Etat partage protege par synchronized |
| LogStore | Journal persistant et reprise |
| ClientApp | Blocking stub et boucle Scanner |

Les threads ordinaires (Thread + Thread.sleep) reprennent le TP Socket : un pour les heartbeats et un pour chaque fenetre de guesses. Pas de pool de threads dans le code applicatif. Les RPC entre noeuds ont un delai de 300 ms ; forwarding 5 s ; client 6 s.
Une operation locale est serialisee avec un verrou : plusieurs clients ne
peuvent pas attribuer le meme seq. Les handlers entrants utilisent le verrou
NodeState et ne gardent pas le verrou d'operation pendant la communication.

Journal UTF-8 append-only : `EPOCH`, `ENTRY` et checkpoints `SNAPSHOT`.
Chaque ENTRY a epoch, seq, op, value, status, client, request, round ; IDs encodes
Base64. Un commit ajoute la version COMMITTED du meme tag. SNAPSHOT contient un
checkpoint Protobuf en Base64, persiste en une ligne complete avant application.
Un checkpoint ne peut pas enlever un commit. Une derniere ligne incomplete est
tronquee au redemarrage ; une ligne complete corrompue bloque la reprise.
`FileChannel.force(true)` precede chaque ACK. Une proposition seule ne modifie
jamais la cible visible.

Perimetre : modele simplifie du TP, cinq membres fixes, reseau localhost stable,
pannes par arret/reprise de processus. Ce n'est pas l'implementation complete de
ZAB ou Raft : pas de votes Raft, pas de garantie generale sur les partitions ou
les delais arbitraires. Des historiques contradictoires bloquent la reprise.
Read est local et peut etre en retard sur un follower avant synchronisation.

## Tests et livrables

```powershell
./scripts/test.ps1
```

17 tests : 9 de stockage, 6 d'etat, 1 scenario complet avec cinq JVM et 1 test du cycle de vie des processus.
Le scenario couvre les trois flux, redemarrage, injection controlee d'une queue
PROPOSED, quorum absent, retry, valeur invalide, ronde obsolete et valeur zero.
Ports des tests : 18501-18505 ; donnees temporaires isolees des donnees de demo.
Les sorties de la derniere execution sont dans `target/demo-evidence`. Le test des processus utilise le port 18601 et conserve ses traces dans `target/process-evidence`. Il verifie le refus d'un demarrage en double, un conflit de port, la fermeture concurrente et la liberation du port et du journal.

- `docs/rapport.md` : rapport francais editable.
- `output/pdf/rapport_zookeeper.pdf` : rapport et captures des traces de tests.
- `docs/答辩讲解.md` : explication chinoise et phrases francaises de soutenance.
- `docs/evidence/` : preuves conservees de l'execution validee.
- `output/TP_ZooKeeper_YuefanLiu.zip` : code, scripts, README, rapport et preuves.
