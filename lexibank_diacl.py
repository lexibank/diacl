# coding=utf-8
"""
https://diacl.ht.lu.se/Content/documents/DiACL-lexicology.pdf
"""
from __future__ import unicode_literals, print_function

from collections import defaultdict, OrderedDict
from itertools import groupby
import gzip
from json import dumps, loads

from tqdm import tqdm
import attr
from clldutils.path import Path, remove
from pycldf.sources import Source
from pylexibank.dataset import Lexeme, Cognate, Concept, Dataset as BaseDataset, Language


def _get_text(e, xpath):
    ee = e.find(xpath)
    if hasattr(ee, 'text'):
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
    id = 'diacl'
    dir = Path(__file__).parent
    concept_class = Concept
    lexeme_class = DiaclLexeme
    cognate_class = DiaclCognate
    language_class = DiaclLanguage

    def _url(self, path):
        return 'https://diacl.ht.lu.se{0}'.format(path)

    def _download_json(self, path):
        target = path.replace('/', '_') + '.json'
        self.raw.download(self._url('/Json/' + path), target)
        return loads(self.raw.read(target))

    def cmd_download(self, **kw):
        """
        Download files to the raw/ directory. You can use helpers methods of `self.raw`, e.g.

        >>> self.raw.download(url, fname)

        $ csvstack -t ../../concepticon/concepticon-dev/concepticondata/conceptlists/Carling-2017-*.tsv > etc/concepts.csv
        """
        #https://diacl.ht.lu.se/GeoJson/GeographicalPresence/24
        print('Download wordlists ...')
        wordlists = self._download_json('WordLists')
        for wlid in tqdm(list(wordlists.keys())):
            # We download the XML representations, because only these seem to contain source info
            # per lexeme.
            self.raw.download(
                self._url('/Xml/WordListWithLanguageLexemes/{0}'.format(wlid)),
                'wl{0}.xml'.format(wlid), skip_if_exists=True)
        print('... done')

        print('Download etymologies ...')
        etymologies_by_wordlistitem = OrderedDict()
        for wl in wordlists.values():
            print(wl['Name'])
            for wlc in wl['WordListCategories'].values():
                print('-- ', wlc['Name'])
                for wli in tqdm(wlc['WordListItems']):
                    data = self._download_json('WordListLexemesWithAncestors/{0}'.format(wli))
                    del data['lexemes']
                    del data['languages']
                    etymologies_by_wordlistitem[wli] = data
        with gzip.GzipFile(str(self.raw.joinpath('etymology.json.gz')), 'w') as fp:
            fp.write(dumps(etymologies_by_wordlistitem).encode('utf8'))
        for p in self.raw.glob('WordListLexemesWithAncestors*'):
            remove(p)
        print('... done')

        self._download_json('LanguageTree')

    def clean_form(self, item, form):
        for f, t in {
            "[sub]1[/sub]": "₁",
            "[sub]2[/sub]": "₂",
            "[sub]3[/sub]": "₃",
            "[sup]h[/sup]": "ʰ",
            "[sup]w[/sup]": "ʷ",
            "[sup]y[/sup]": "ʸ",
            "[sup][/sup]": "",
        }.items():
            form = form.replace(f, t)
        return form

    def split_forms(self, item, value):
        # We only take the first form, since the proliferation of variants seems to be rather
        # unprincipled, otherwise.
        return [value.split(',')[0].split(';')[0].strip()]

    def cmd_install(self, **kw):
        """
        Convert the raw data to a CLDF dataset.

        Use the methods of `pylexibank.cldf.Dataset` after instantiating one as context:

        >>> with self.cldf as ds:
        ...     ds.add_sources(...)
        ...     ds.add_language(...)
        ...     ds.add_concept(...)
        ...     ds.add_lexemes(...)
        """
        glottocode_map = {int(l['ID']): l['Glottocode'] for l in self.languages if l['Glottocode']}

        concepts, concept_map = OrderedDict(), {}
        for cid, items in groupby(
                sorted(self.concepts, key=lambda c_: c_['CONCEPTICON_ID']),
                lambda c_: c_['CONCEPTICON_ID']):
            for item in items:
                concepts[cid] = item['CONCEPTICON_GLOSS']
                concept_map[int(item['DIACL_ID'])] = cid

        broader_concept_map = {}
        for cid in list(concepts.keys()):
            for relcid, _ in self.concepticon.relations.iter_related(cid, 'broader'):
                if relcid in concepts:
                    broader_concept_map[cid] = relcid
                    break

        tree = parse_tree(loads(self.raw.read('LanguageTree.json')))
        complete_families = [
            235,  # Chapacuran
            245,  # Indo-European
            469,  # Kartvelian
            478,  # Nambikwaran
            #Romani chib ?
            506,  # Tupían
            483,  # Caucasian languages - see https://twitter.com/DiachronicAtlas/status/970650946689781761
        ]

        wls = [
            self.raw.read_xml(p.name, wrap=False) for p in
            sorted(self.raw.glob('wl*.xml'), key=lambda p_: int(p_.stem[2:]))]
        languages, lexemes, sources = {}, {}, {}
        for wl in wls:
            for src in wl.findall('./sources/source'):
                src = parse_source(src)
                sources[int(src.id)] = src

            for lang in wl.findall('.//language'):
                languages[int(lang.get('language-id'))] = parse_language(lang)
                for lex in lang.findall('.//lexeme'):
                    lexemes[int(lex.get('lexeme-id'))] = parse_lexeme(lex)

        # Compute cognate sets (or etymologies) as union of all lexemes ocurring together in any
        # EtymologyTree:
        cogsets = []
        with gzip.GzipFile(str(self.raw.joinpath('etymology.json.gz')), 'r') as fp:
            # we have to cluster using etymologies where FkReliabilityId < 2
            # we also assume cognacy to be transitive
            for wli, data in loads(fp.read().decode('utf8')).items():
                if not concept_map[int(wli)]:
                    print('skipping word list item {0}'.format(wli))
                    continue

                for e in data['etymologies'].values():
                    if e['FkReliabilityId'] < 2:
                        found = False
                        for c in cogsets:
                            if e['FkChildId'] in c:
                                c.add(e['FkParentId'])
                                found = True
                            if e['FkParentId'] in c:
                                c.add(e['FkChildId'])
                                found = True
                        if not found:
                            cogsets.append({e['FkParentId'], e['FkChildId']})
                for lid in data['connectedLexemesById']:
                    lexemes[lid]['concepts'].add(concept_map[int(wli)])

        # remove duplicates:
        cogsets = set([tuple(sorted(c)) for c in cogsets])

        # Augment sets of lexeme ids with associated (broadened) concepts:
        cogsets = [set(
            (l, frozenset(broader_concept_map.get(ci, ci) for ci in lexemes[l]['concepts']))
             for l in c if l in lexemes and lexemes[l]['concepts']) for c in cogsets]

        # Partition into sets of lexemes with identical concepts.
        # Note: One cognate set has lexemes associated with two concepts:
        # "man (adult male human)" and "person (individual human)"
        cogsets_ = []
        for c in cogsets:
            for cids, items in groupby(sorted(c, key=lambda i: i[1]), lambda i: i[1]):
                cogsets_.append((cids, frozenset(i[0] for i in items)))
        cogsets = [cs for cs in cogsets_ if len(cs[1]) > 1]

        # Remove partial cognate sets, contained in a larger one:
        cogsets_ = set()
        for cs in sorted(cogsets, key=lambda s: len(s[1]), reverse=True):
            for c in cogsets_:
                if c[0] == cs[0] and cs[1].issubset(c[1]):
                    break
            else:
                cogsets_.add(cs)

        cogsets = {
            i: c for i, c in
            enumerate(sorted(cogsets_, key=lambda cs: tuple(sorted(cs[1]))), start=1)}
        lex_to_cogid = defaultdict(set)
        for csid, (cids, lids) in cogsets.items():
            if len(cids) > 1:
                print('++cognate set for multiple concepts++', cids, lids)
            for lid in lids:
                lex_to_cogid[(lid, cids)].add(csid)
        for k, v in lex_to_cogid.items():
            if len(v) > 1:
                print('--lexeme in multiple cognate sets--', k, v)

        with self.cldf as ds:
            lexemes = {k: v for k, v in lexemes.items() if v['concepts']}

            for cid, gloss in concepts.items():
                if cid:
                    ds.add_concept(ID=cid, Name=gloss, Concepticon_ID=cid)

            for src in sorted(sources.values(), key=lambda s: s['key']):
                ds.add_sources(src)

            lids = set(l['language-id'] for l in lexemes.values())
            for lid, lang in sorted(languages.items()):
                if lid in lids:
                    ds.add_language(
                        ID=lid,
                        Name=lang['name'],
                        Glottocode=glottocode_map.get(lid, self.glottolog.glottocode_by_iso.get(lang['iso-693-3'])),
                        ISO639P3code=lang['iso-693-3'],
                        Latitude=lang.get('latitude'),
                        Longitude=lang.get('longitude'),
                        time_frame=lang.get('time_frame')
                    )

            for lid, lex in sorted(lexemes.items()):
                for cid in lex['concepts']:
                    for l in ds.add_lexemes(
                        Value=lex['form-transcription'],
                        Language_ID=lex['language-id'],
                        Parameter_ID=cid,
                        diacl_id=lid,
                        Source=[s[0] for s in lex['sources']],
                        transliteration=lex['form-transliteration'],
                        ipa=lex['form-ipa'],
                        meaning=lex['meaning'],
                        meaning_note=lex['meaning_note'],
                    ):
                        for csid in lex_to_cogid.get((lid, frozenset([cid])), []):
                            ds.add_cognate(lexeme=l, Cognateset_ID=csid, diacl_lexeme_id=lid)


def parse_language(l):
    res = {a: _get_text(l, a) for a in ['name', 'alternative-names', 'note', 'native-speakers', 'iso-693-3']}
    fp = l.find('focal-point')
    if fp is not None:
        res['latitude'] = _get_text(fp, 'wgs84-latitude')
        res['longitude'] = _get_text(fp, 'wgs84-longitude')
    fp = l.find('time-frame')
    if fp is not None:
        res['time-frame'] = '{0}-{1}'.format(_get_text(fp, 'from'), _get_text(fp, 'until'))
    return res


def parse_lexeme(l):
    res = {a: _get_text(l, a) for a in ['language-id', 'form-transcription', 'form-transliteration', 'form-ipa', 'note']}
    res['language-id'] = int(res['language-id'])
    res['concepts'] = set()
    res['meaning'] = _get_text(l.find('semantics'), 'meaning')
    res['meaning_note'] = _get_text(l.find('semantics'), 'note')
    res['sources'] = [(_get_text(s, 'source-id'), _get_text(s, 'location-within-source')) for s in l.findall('.//source')]
    return res


def parse_source(src):
    sid = src.get('source-id')
    if _get_text(src, 'type') == 'Informant':
        kw = dict(howpublished='Informant: {0}'.format(_get_text(src, 'full-citation')))
    else:
        kw = dict(howpublished=_get_text(src, 'full-citation'), note=_get_text(src, 'note'))
    kw['key'] = _get_text(src, 'citation-key')
    return Source('misc', sid, **kw)


def lids(node):
    if 'FkLanguageId' in node:
        yield node['FkLanguageId']
    for n in node.get('children', []):
        for lid in lids(n):
            yield lid


def iternodes(node, level=0):
    yield level, node
    for n in node.get('children', []):
        for l, nn in iternodes(n, level=level + 1):
            yield l, nn


def parse_tree(tree):
    """
    :return: `dict` mapping (NodeName, NodeId) pairs to lists of `FkLanguageId`s
    """
    res = {}
    for l, n in iternodes(tree):
        res[n['NodeId']] = (l, n['NodeName'], list(lids(n)))
    return res
