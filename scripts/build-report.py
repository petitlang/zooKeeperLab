"""Prepare local evidence pages (--evidence), then build the report from captures.

Requires reportlab and pypdf. Screenshots are captured separately from the browser.
The evidence is copied from a successful test run, never invented here.
"""
from pathlib import Path
from html import escape
import shutil
import sys

ROOT = Path(__file__).resolve().parent.parent
EVIDENCE = ROOT / "docs" / "evidence"


def prepare_evidence():
    for file in (ROOT / "target" / "demo-evidence").iterdir():
        if file.is_file():
            shutil.copy2(file, EVIDENCE / file.name)
    for file in (ROOT / "target" / "surefire-reports").glob("*.txt"):
        shutil.copy2(file, EVIDENCE / file.name)
    text = (EVIDENCE / "scenarios.txt").read_text(encoding="utf-8")
    if "ALL ASSERTIONS PASSED" not in text:
        raise RuntimeError("Report requires a successful cluster run.")
    blocks = text.strip().split("\n\n")
    node1 = (EVIDENCE / "node-1.log").read_text(encoding="utf-8")
    write_log = [line for line in node1.splitlines()
                 if "(1,1)" in line and ("PROPOSE" in line or "ACK" in line or "COMMIT" in line)]
    guess_log = [line for line in node1.splitlines()
                 if "GUESS client=" in line or "WINNER" in line
                 or "PROPOSE (1,2)" in line or "COMMIT (1,2)" in line]
    panels = {
        "write": ("1. Write via un follower", "\n".join(write_log) + "\n\n" + blocks[2]),
        "guess": ("2. Deux guesses et nouvelle ronde", "\n".join(guess_log) + "\n\n" + blocks[3]),
        "crash": ("3. Crash, nouvelle election et redemarrage", "\n\n".join(
            "\n".join(block.splitlines()[:3]) for block in blocks[4:7])),
        "recovery": ("Tests supplementaires : reprise et quorum", "\n\n".join(blocks[8:10])),
    }
    for name, (title, content) in panels.items():
        # Same content, wrapped onto two lines for readability in the report.
        content = content.replace(" target=", "\n    target=")
        html = f'''<!doctype html><html lang="fr"><meta charset="utf-8">
<title>{escape(title)}</title><style>
*{{box-sizing:border-box}}body{{margin:0;background:#fff;color:#172033;
font-family:Arial,sans-serif;width:980px;padding:24px}}h1{{font-size:26px;margin:0 0 8px}}
p{{font-size:16px;color:#536070;margin:0 0 20px}}pre{{white-space:pre-wrap;
font:18px/1.35 Consolas,monospace;border:1px solid #ccd3dc;background:#f5f7fa;
padding:18px;margin:0}}footer{{font-size:14px;margin-top:12px;color:#536070}}
</style><h1>{escape(title)}</h1>
<p>Execution validee le 13 octobre 2026 - cinq JVM independantes, gRPC localhost.</p>
<pre>{escape(content)}</pre>
<footer>Capture de traces reelles affichees sur une page locale. Sources : scenarios.txt et node-*.log.</footer>
</html>'''
        (EVIDENCE / f"{name}.html").write_text(html, encoding="utf-8")


def build_pdf():
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_LEFT
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from PIL import Image as PILImage

    pdfmetrics.registerFont(TTFont("Arial", "C:/Windows/Fonts/arial.ttf"))
    pdfmetrics.registerFont(TTFont("ArialBold", "C:/Windows/Fonts/arialbd.ttf"))
    pdfmetrics.registerFontFamily("Arial", normal="Arial", bold="ArialBold")
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="BodyLab", fontName="Arial", fontSize=10.5, leading=15, spaceAfter=10))
    styles.add(ParagraphStyle(name="TitleLab", fontName="ArialBold", fontSize=24, leading=29, spaceAfter=16))
    styles.add(ParagraphStyle(name="HeadingLab", fontName="ArialBold", fontSize=13, leading=18, spaceBefore=7, spaceAfter=9))
    styles.add(ParagraphStyle(name="SmallLab", fontName="Arial", fontSize=8.7, leading=12, spaceAfter=9))
    styles.add(ParagraphStyle(name="CellLab", fontName="Arial", fontSize=9, leading=12))
    story = []
    markdown = ["# TP ZooKeeper-like - Yuefan Liu", "", "II.3502 - 13 octobre 2026", ""]
    width = A4[0] - 88

    def title(text):
        story.append(Paragraph(escape(text), styles["TitleLab"]))
        markdown.extend(["## " + text, ""])

    def heading(text):
        story.append(Paragraph(escape(text), styles["HeadingLab"]))
        markdown.extend(["### " + text, ""])

    def para(text, small=False):
        story.append(Paragraph(escape(text), styles["SmallLab" if small else "BodyLab"]))
        markdown.extend([text, ""])

    def table(rows, widths):
        cells = [[Paragraph(escape(str(cell)), styles["CellLab"]) for cell in row] for row in rows]
        t = Table(cells, colWidths=widths, hAlign="LEFT")
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e5ebf2")),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#ccd3dc")),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 9),
            ("RIGHTPADDING", (0, 0), (-1, -1), 9),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ]))
        story.extend([t, Spacer(1, 12)])
        for row in rows:
            markdown.append("| " + " | ".join(map(str, row)) + " |")
            if row is rows[0]:
                markdown.append("| " + " | ".join("---" for _ in row) + " |")
        markdown.append("")

    def figure(name, caption):
        path = EVIDENCE / f"{name}.png"
        with PILImage.open(path) as im:
            w, h = im.size
        scale = min(width / w, 370 / h)
        story.extend([Image(str(path), width=w * scale, height=h * scale, hAlign="LEFT"), Spacer(1, 7)])
        para(caption, small=True)
        markdown.extend([f"![{caption}](evidence/{name}.png)", ""])

    title("Système ZooKeeper-like")
    para("Yuefan Liu - II.3502 Distributed Architecture and Programming - 13 octobre 2026", small=True)
    heading("Objectif et continuité avec les TP précédents")
    para("Le programme utilise cinq serveurs Java indépendants sur localhost. Un leader coordonne les écritures, quatre followers répliquent le journal. Les clients peuvent contacter n'importe quel nœud vivant pour écrire, proposer une guess ou lire la cible.")
    para("Je garde la structure du TP gRPC Calculator : définition des messages dans un fichier .proto, ServerBuilder pour démarrer le serveur, ManagedChannel et blocking stub pour appeler un service, puis StreamObserver pour retourner la réponse. Le menu Scanner et les threads ordinaires reprennent le principe du TP Socket. La nouveauté est la coordination entre plusieurs serveurs.")
    heading("Organisation du code")
    table([
        ["Composant", "Responsabilité"],
        ["NodeServer / NodeRuntime", "Serveur, connexions, composants et threads de fond."],
        ["GameServiceImpl / ClientApp", "Write, Guess, Read ; transfert au leader ; menu client."],
        ["LeaderCoordinator", "PROPOSE, ACK durables, quorum et COMMIT."],
        ["CoordinationServiceImpl", "Réception des messages entre nœuds via gRPC."],
        ["ElectionManager", "Heartbeat, comparaison des tags et synchronisation."],
        ["GameEngine", "Fenêtre de guesses et sélection par distance absolue."],
        ["NodeState / LogStore", "État partagé, journal persistant et reprise."],
    ], [145, width - 145])
    heading("Choix simples")
    para("Le quorum est fixé à trois sur cinq. Les appels entre nœuds ont un délai de 300 ms. Le leader envoie des heartbeats avec un intervalle de 500 ms après chaque cycle ; le timeout est de cinq secondes plus un petit décalage par ID. Un verrou sérialise les écritures et les élections locales.")
    story.append(PageBreak())

    title("1. Écriture normale")
    para("Le client A envoie Write(50) au follower 4. Celui-ci transmet la requête au leader sans modifier sa cible. Le leader attribue le tag (1,1), écrit PROPOSED sur disque, puis envoie Propose aux quatre followers. L'ACK est la réponse RPC, envoyée après persistance du proposal.")
    para("Le leader compte sa propre copie et les ACK valides. À partir de trois copies durables, il commit localement et diffuse COMMIT. La cible et la ronde changent seulement après le commit. Les cinq réponses Read finissent avec target=50, round=1 et tag=(1,1).")
    figure("write", "Figure 1. Traces réelles du leader et lectures des cinq nœuds après Write(50).")
    heading("Persistance et concurrence")
    para("Chaque journal contient epoch, seq, op, value, status, client, request et round. Les écritures sont forcées sur disque avant l'ACK. Un commit ajoute une version COMMITTED du même tag. Un checkpoint SNAPSHOT en une ligne permet la synchronisation sans retirer les commits existants.")
    para("NodeState utilise synchronized pour protéger les variables partagées. Deux écritures concurrentes ne peuvent pas obtenir le même seq. Une même requête client/ID peut être rejouée sans produire une seconde transaction ; le menu affiche l'ID mais crée un nouvel ID à chaque commande.")
    story.append(PageBreak())

    title("2. Jeu de guesses")
    para("Avec la cible 50, le client B propose 30 via le nœud 3 et le client C propose 62 via le nœud 5. Le leader collecte pendant trois secondes à partir de la première guess. Une guess acceptée reste en mémoire et ne change pas immédiatement la cible.")
    para("La distance de 30 est |30-50|=20, celle de 62 est |62-50|=12. Le leader choisit donc 62 et crée une opération GUESS qui suit le même cycle PROPOSE, ACK, COMMIT. Les lectures indiquent alors target=62, round=2 et tag=(1,2).")
    figure("guess", "Figure 2. Deux guesses collectées, sélection de 62 et cible identique sur les cinq nœuds.")
    heading("Règles ajoutées pour terminer une ronde")
    para("La consigne ne fixe pas le moment où toutes les guesses sont reçues. La fenêtre de trois secondes rend la démonstration simple. Un client soumet au plus une guess par ronde ; à distance égale, la première arrivée gagne. Le client lit la ronde avant de soumettre et le serveur refuse une ronde obsolète.")
    para("REQUEST_ACCEPTED signifie que la guess est en attente ; REQUEST_COMMITTED signifie qu'une transaction a été commise. Write peut changer la cible pour préparer un autre exemple. Un changement de ronde annule les guesses de l'ancienne ronde.")
    story.append(PageBreak())

    title("3. Crash et reprise")
    para("Le test termine de force le processus du leader 1. Les followers détectent l'absence de heartbeat. Ils échangent leur dernier tag, y compris les proposals non commis. Le plus grand (epoch, seq) gagne ; à tag égal, le plus petit ID gagne. Dans cette exécution, le nœud 2 devient leader.")
    para("Le nouveau leader rassemble au moins trois journaux compatibles, préserve les commits et synchronise la queue sélectionnée. Les proposals restants sont re-répliqués à quorum avant leur commit. Il persiste un nouvel epoch et remet seq à zéro. Write(40) utilise ensuite (2,1). Le nœud 1 redémarre avec son ancien journal, reste follower et rattrape la cible 40.")
    figure("crash", "Figure 3. Leader 2 après crash, Write(40) dans le nouvel epoch et retour du nœud 1 en follower.")
    para("Pendant la reprise, les écritures normales sont refusées. Les guesses seulement en mémoire doivent être soumises de nouveau. Un proposal déjà enregistré peut être terminé pendant la reprise : une réponse REQUEST_UNCERTAIN ne signifie donc pas que l'opération a définitivement échoué.")
    story.append(PageBreak())

    title("Validation et limites")
    para("La commande scripts/test.ps1 passe 17 tests sans échec : neuf de stockage, six d'état, un scénario avec cinq JVM et un test du cycle de vie des processus. Les trois flux, le démarrage en double, le conflit de port et la fermeture concurrente sont vérifiés. Après fermeture, le port et le verrou du journal sont libérés. Les traces sont conservées dans docs/evidence.")
    figure("recovery", "Figure 4. Reprise du proposal 77, puis impossibilité de commit avec seulement deux nœuds vivants.")
    para("Pour vérifier la queue non commise de façon reproductible, le test injecte volontairement un proposal 77 sur deux followers après l'arrêt du leader 2. La cible reste 40 avant la reprise. Le nœud 3 possède le meilleur tag ; il gagne, synchronise à quorum et commit 77. Cette injection contrôlée est distincte du crash normal.")
    para("Le test vérifie aussi les retries, le refus de 101, le refus d'une ronde obsolète et le fait que zéro est une cible valide. Avec deux nœuds, Write(18) n'est pas commis immédiatement ; une fois le troisième revenu, la reprise peut terminer ce proposal incertain.")
    heading("Périmètre")
    para("C'est un modèle simplifié pour cinq membres fixes, un réseau localhost stable et des arrêts/reprises de processus. Il ne réalise pas tous les mécanismes de ZAB ou Raft et ne garantit pas le consensus pour des partitions ou délais arbitraires. Des journaux contradictoires bloquent la reprise. Read est local : un follower en retard peut rendre un ancien commit avant synchronisation.")
    para("Les captures montrent une page locale affichant les sorties réelles du test, et non un terminal interactif. La démonstration manuelle se fait avec les commandes du README. Source des exigences : lab_zookeeper.pdf, II.3502, 2026/27 ; structure de communication reprise des TP gRPC-Protobuf et Sockets de Yuefan Liu.", small=True)

    output = ROOT / "output" / "pdf" / "rapport_zookeeper.pdf"
    output.parent.mkdir(parents=True, exist_ok=True)
    def footer(canvas, doc):
        canvas.setFont("Arial", 8)
        canvas.setFillColor(colors.HexColor("#536070"))
        canvas.drawString(44, 26, "Yuefan Liu - TP ZooKeeper-like - 13 octobre 2026")
        canvas.drawRightString(A4[0] - 44, 26, str(doc.page))
    doc = SimpleDocTemplate(str(output), pagesize=A4, rightMargin=44, leftMargin=44,
                           topMargin=40, bottomMargin=44, title="TP ZooKeeper-like", author="Yuefan Liu")
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    (ROOT / "docs" / "rapport.md").write_text("\n".join(markdown), encoding="utf-8")
    print(output)


if __name__ == "__main__":
    if "--evidence" in sys.argv:
        prepare_evidence()
    else:
        build_pdf()
