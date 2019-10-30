"""
https://diacl.ht.lu.se/Content/documents/DiACL-lexicology.pdf
"""
import gzip
from collections import OrderedDict
from itertools import groupby
from json import dumps, loads
from pathlib import Path

import attr
from pycldf.sources import Source
from pylexibank import Lexeme, Cognate, Concept, Language
from pylexibank.dataset import Dataset as BaseDataset
from pylexibank.forms import FormSpec
from pylexibank.util import progressbar


def _get_text(e, xpath):
    ee = e.find(xpath)
    if hasattr(ee, "text"):
        return ee.text


@attr.s
class DiaclLexeme(Lexeme):
    diacl_id = attr.ib(default=None)
    meaning = attr.ib(default=None)
    meaning_note = attr.ib(default=None)
    transliteration = attr.ib(default=None)
    ipa = attr.ib(default=None)


@attr.s
class DiaclCognate(Cognate):
    diacl_lexeme_id = attr.ib(default=None)


@attr.s
class DiaclLanguage(Language):
    time_frame = attr.ib(default=None)
    Longitude = attr.ib(default=None)
    Latitude = attr.ib(default=None)


class Dataset(BaseDataset):
    id = "diacl"
    dir = Path(__file__).parent
    concept_class = Concept
    lexeme_class = DiaclLexeme
    cognate_class = DiaclCognate
    language_class = DiaclLanguage

    replacements = {
        "[sub]1[/sub]": "₁",
        "[sub]2[/sub]": "₂",
        "[sub]3[/sub]": "₃",
        "[sup]h[/sup]": "ʰ",
        "[sup]w[/sup]": "ʷ",
        "[sup]y[/sup]": "ʸ",
        "[sup][/sup]": "",
    }

    form_spec = FormSpec(
        separators=";,",
        strip_inside_brackets=False,
        replacements=replacements,
        first_form_only=True,
    )

    @staticmethod
    def _url(path):
        return "https://diacl.ht.lu.se{0}".format(path)

    def _download_json(self, path):
        target = path.replace("/", "_") + ".json"
        self.raw_dir.download(self._url("/Json/" + path), target)
        return loads(self.raw_dir.read(target))

    def cmd_download(self, args):
        # https://diacl.ht.lu.se/GeoJson/GeographicalPresence/24
        print("Download wordlists ...")
        wordlists = self._download_json("WordLists")
        for wlid in progressbar(list(wordlists.keys())):
            # We download the XML representations, because only these seem to contain source info
            # per lexeme.
            self.raw_dir.download(
                self._url("/Xml/WordListWithLanguageLexemes/{0}".format(wlid)),
                "wl{0}.xml".format(wlid),
                skip_if_exists=True,
            )
        print("... done")

        print("Download etymologies ...")
        etymologies_by_wordlistitem = OrderedDict()
        for wl in wordlists.values():
            print(wl["Name"])
            for wlc in wl["WordListCategories"].values():
                print("-- ", wlc["Name"])
                for wli in progressbar(wlc["WordListItems"]):
                    data = self._download_json("WordListLexemesWithAncestors/{0}".format(wli))
                    del data["lexemes"]
                    del data["languages"]
                    etymologies_by_wordlistitem[wli] = data
        with gzip.GzipFile(str(self.raw_dir.joinpath("etymology.json.gz")), "w") as fp:
            fp.write(dumps(etymologies_by_wordlistitem).encode("utf8"))
        for p in self.raw_dir.glob("WordListLexemesWithAncestors*"):
            Path.unlink(p)
        print("... done")

        self._download_json("LanguageTree")

    def cmd_makecldf(self, args):
        glottocode_map = {int(l["ID"]): l["Glottocode"] for l in self.languages if l["Glottocode"]}
        lmap = {int(l["ID"]): l for l in self.languages}

        concepts, concept_map = OrderedDict(), {}
        for cid, items in groupby(
            sorted(self.concepts, key=lambda c_: c_["CONCEPTICON_ID"]),
            lambda c_: c_["CONCEPTICON_ID"],
        ):
            for item in items:
                concepts[cid] = item["CONCEPTICON_GLOSS"]
                concept_map[int(item["DIACL_ID"])] = cid

        wls = [
            self.raw_dir.read_xml(p.name, wrap=False)
            for p in sorted(self.raw_dir.glob("wl*.xml"), key=lambda p_: int(p_.stem[2:]))
        ]
        languages, lexemes, sources = {}, {}, {}
        for wl in wls:
            for src in wl.findall("./sources/source"):
                src = parse_source(src)
                sources[int(src.id)] = src

            for lang in wl.findall(".//language"):
                languages[int(lang.get("language-id"))] = parse_language(lang)
                for lex in lang.findall(".//lexeme"):
                    lexemes[int(lex.get("lexeme-id"))] = parse_lexeme(lex)

        with gzip.GzipFile(str(self.raw_dir.joinpath("etymology.json.gz")), "r") as fp:
            # we have to cluster using etymologies where FkReliabilityId < 2
            # we also assume cognacy to be transitive
            for wli, data in loads(fp.read().decode("utf8")).items():
                if not concept_map[int(wli)]:
                    print("skipping word list item {0}".format(wli))
                    continue

                for lid in data["connectedLexemesById"]:
                    lexemes[lid]["concepts"].add(concept_map[int(wli)])

        lexemes = {k: v for k, v in lexemes.items() if v["concepts"]}

        for cid, gloss in concepts.items():
            if cid:
                args.writer.add_concept(ID=cid, Name=gloss, Concepticon_ID=cid)

        for src in sorted(sources.values(), key=lambda s: s["key"]):
            args.writer.add_sources(src)

        lids = set(l["language-id"] for l in lexemes.values())
        for lid, lang in sorted(languages.items()):
            if lid in lids:
                if lid in lmap:
                    for attr in ["Latitude", "Longitude"]:
                        if lmap[lid][attr]:
                            lang[attr.lower()] = lmap[lid][attr]
                args.writer.add_language(
                    ID=lid,
                    Name=lang["name"],
                    Glottocode=glottocode_map.get(
                        lid, self.glottolog.glottocode_by_iso.get(lang["iso-693-3"])
                    ),
                    ISO639P3code=lang["iso-693-3"],
                    Latitude=lang.get("latitude"),
                    Longitude=lang.get("longitude"),
                    time_frame=lang.get("time_frame"),
                )

        for lid, lex in sorted(lexemes.items()):
            for cid in sorted(lex["concepts"]):
                args.writer.add_forms_from_value(
                    Value=lex["form-transcription"],
                    Language_ID=lex["language-id"],
                    Parameter_ID=cid,
                    diacl_id=lid,
                    Source=[s[0] for s in lex["sources"]],
                    transliteration=lex["form-transliteration"],
                    ipa=lex["form-ipa"],
                    meaning=lex["meaning"],
                    meaning_note=lex["meaning_note"],
                )


def parse_language(l):
    res = {
        a: _get_text(l, a)
        for a in ["name", "alternative-names", "note", "native-speakers", "iso-693-3"]
    }
    fp = l.find("focal-point")
    if fp is not None:
        res["latitude"] = _get_text(fp, "wgs84-latitude")
        res["longitude"] = _get_text(fp, "wgs84-longitude")
    fp = l.find("time-frame")
    if fp is not None:
        res["time-frame"] = "{0}-{1}".format(_get_text(fp, "from"), _get_text(fp, "until"))
    return res


def parse_lexeme(l):
    res = {
        a: _get_text(l, a)
        for a in ["language-id", "form-transcription", "form-transliteration", "form-ipa", "note"]
    }
    res["language-id"] = int(res["language-id"])
    res["concepts"] = set()
    res["meaning"] = _get_text(l.find("semantics"), "meaning")
    res["meaning_note"] = _get_text(l.find("semantics"), "note")
    res["sources"] = [
        (_get_text(s, "source-id"), _get_text(s, "location-within-source"))
        for s in l.findall(".//source")
    ]
    return res


def parse_source(src):
    sid = src.get("source-id")
    if _get_text(src, "type") == "Informant":
        kw = dict(howpublished="Informant: {0}".format(_get_text(src, "full-citation")))
    else:
        kw = dict(howpublished=_get_text(src, "full-citation"), note=_get_text(src, "note"))
    kw["key"] = _get_text(src, "citation-key")
    return Source("misc", sid, **kw)


def lids(node):
    if "FkLanguageId" in node:
        yield node["FkLanguageId"]
    for n in node.get("children", []):
        for lid in lids(n):
            yield lid


def iternodes(node, level=0):
    yield level, node
    for n in node.get("children", []):
        for l, nn in iternodes(n, level=level + 1):
            yield l, nn


def parse_tree(tree):
    """
    :return: `dict` mapping (NodeName, NodeId) pairs to lists of `FkLanguageId`s
    """
    res = {}
    for l, n in iternodes(tree):
        res[n["NodeId"]] = (l, n["NodeName"], list(lids(n)))
    return res
