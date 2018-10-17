# coding=utf-8
from __future__ import unicode_literals, print_function

import re
from collections import defaultdict, Counter, OrderedDict
from itertools import groupby
import gzip
from json import dumps, loads

import attr
from clldutils.path import Path, remove
from clldutils.jsonlib import load
from csvw.dsv import UnicodeWriter
from pycldf.sources import Source
from pylexibank.dataset import Lexeme, Cognate, Concept, Dataset as BaseDataset


def _get_text(e, xpath):
    ee = e.find(xpath)
    if hasattr(ee, 'text'):
        return ee.text


@attr.s
class DiaclLexeme(Lexeme):
    diacl_id = attr.ib(default=None)


@attr.s
class DiaclCognate(Cognate):
    diacl_lexeme_id = attr.ib(default=None)


@attr.s
class DiaclConcept(Concept):
    word_list_category = attr.ib(default=None)


class Dataset(BaseDataset):
    id = 'diacl'
    dir = Path(__file__).parent
    concept_class = DiaclConcept
    lexeme_class = DiaclLexeme
    cognate_class = DiaclCognate

    def _url(self, path):
        return 'https://diacl.ht.lu.se{0}'.format(path)

    def _iter_langs(self):
        for lang in self.raw.read_xml('languages.xml', wrap=False).findall('.//language'):
            yield lang.get('language-id'), lang

    def cmd_download(self, **kw):
        """
        Download files to the raw/ directory. You can use helpers methods of `self.raw`, e.g.

        >>> self.raw.download(url, fname)
        """
        #https://diacl.ht.lu.se/GeoJson/GeographicalPresence/24
        self.raw.download(self._url('/WordList/Index'), 'wordlists.html')
        for p in re.findall('/Xml/WordListWithLanguageLexemes/[0-9]+', self.raw.read('wordlists.html')):
            self.raw.download(self._url(p), 'wl{0}.xml'.format(p.split('/')[-1]))
        self.raw.download(self._url('/Xml/AllLanguages'), 'languages.xml')
        for id_, _ in self._iter_langs():
            target = 'l{0}'.format(id_)
            self.raw.download(self._url('/Xml/SingleLanguageWithLexemes/{0}'.format(id_)), target)
            for lex in self.raw.read_xml(target, wrap=False).findall('.//lexeme'):
                lid = lex.get('lexeme-id')
                target = '{0}.json'.format(lid)
                self.raw.download(self._url('/Json/EtymologyTree/{0}'.format(lid)), target)
        etymologytrees = OrderedDict()
        for p in sorted(self.raw.glob('*.json'), key=lambda p: int(p.stem)):
            etymologytrees[p.stem] = load(p)
            remove(p)
        with gzip.GzipFile(str(self.raw.joinpath('etymology.json.gz')), 'w') as fp:
            fp.write(dumps(etymologytrees).encode('utf8'))

    def split_forms(self, item, value):
        return [value]

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
        # Compute cognate sets (or etymologies) as union of all lexemes ocurring together in any
        # EtymologyTree:
        with gzip.GzipFile(str(self.raw.joinpath('etymology.json.gz')), 'r') as fp:
            cogsets = [frozenset(v['lexemes'].keys()) for v in loads(fp.read().decode('utf8')).values()]

        # Remove cognate sets of length 1:
        cogsets = [cs for cs in cogsets if len(cs) > 1]

        # Remove partial cognate sets, contained in a larger one:
        cogsets_ = set()
        for cs in sorted(cogsets, key=lambda s: len(s), reverse=True):
            for c in cogsets_:
                if cs.issubset(c):
                    break
            else:
                cogsets_.add(cs)

        # Now assign cognate set IDs by enumerating ordered cognate sets:
        in_cogset = defaultdict(list)
        for i, cs in enumerate(sorted(tuple(sorted(s)) for s in cogsets_), start=1):
            for lid in cs:
                in_cogset[lid].append(i)

        languages, concepts = [], []
        no_concept = Counter()
        with self.cldf as ds:
            concept_map = defaultdict(set)
            for p in self.raw.glob('wl*.xml'):
                for wlc in self.raw.read_xml(p.name, wrap=False).findall('.//word-list-category'):
                    wlcname = _get_text(wlc, 'name')
                    for concept in wlc.findall('.//word-list-item'):
                        gloss = concept.find('name').text
                        id_ = concept.get('word-list-item-id')
                        ds.add_concept(ID=id_, Name=gloss, word_list_category=wlcname)
                        concepts.append((id_, gloss, '', wlcname))
                        for lex in concept.findall('.//lexeme-id'):
                            concept_map[lex.text].add(id_)

            cognates = []
            for id_, lang in self._iter_langs():
                """
                <language language-id="30">
                    <name>Syriac</name>
                    <alternative-names>Ancient Syriac, Classical Syriac, Lishana Atiga, Suryaya, Suryoyo</alternative-names>
                    <note/>
                    <native-speakers/>
                    <iso-693-3>syc</iso-693-3>
                    <focal-point>
                        <wgs84-latitude>37.145141</wgs84-latitude>
                        <wgs84-longitude>38.824426</wgs84-longitude>
                    </focal-point>
                    <time-frame>
                        <from/>
                        <until>1250</until>
                    </time-frame>
                    <focus-area-id>550</focus-area-id>
                    <language-area-id>400</language-area-id>
                    <reliability-id>20</reliability-id>
                    <geographical-presences/>
                </language>
                """
                iso = _get_text(lang, 'iso-693-3')  # sic! This seems to be a typo in the database column name!
                glottocode = self.glottolog.glottocode_by_iso.get(iso)
                ds.add_language(
                    ID=id_,
                    Name=lang.find('name').text,
                    Glottocode=glottocode,
                    ISO639P3code=iso)
                languages.append(
                    [id_, glottocode or ''] + [
                        _get_text(lang, p) for p in
                        ['name', 'iso-693-3', './/wgs84-latitude', './/wgs84-longitude', 'alternative-names']])
                ldata = self.raw.read_xml('l{0}'.format(id_), wrap=False)
                for src in ldata.findall('./sources/source'):
                    sid = src.get('source-id')
                    if _get_text(src, 'type') == 'Informant':
                        kw = dict(howpublished='Informant: {0}'.format(_get_text(src, 'full-citation')))
                    else:
                        kw = dict(howpublished=_get_text(src, 'full-citation'), note=_get_text(src, 'note'))
                    ds.add_sources(Source('misc', sid, **kw))
                for lex in ldata.findall('.//lexeme'):
                    form = _get_text(lex, 'form-transcription')
                    if (not form) or form == '---':
                        continue
                    lid = lex.get('lexeme-id')
                    if lid not in concept_map:
                        no_concept.update([_get_text(lex, './/meaning')])
                    else:
                        for cid in concept_map[lid]:
                            for l in ds.add_lexemes(
                                Value=form,
                                Language_ID=id_,
                                Parameter_ID=cid,
                                diacl_id=lid,
                                Source=[_get_text(src, 'source-id') for src in lex.findall('sources/source')],
                            ):
                                for csid in in_cogset.get(lid, []):
                                    cognates.append((csid, l))

            for csid, items in groupby(sorted(cognates, key=lambda i: i[0]), lambda i: i[0]):
                items = list(items)
                if len(set(i[1]['diacl_id'] for i in items)) < 2:
                    continue
                for _, lex in items:
                    ds.add_cognate(lexeme=lex, Cognateset_ID=csid, diacl_lexeme_id=lex['diacl_id'])

        print(sum(list(no_concept.values())))
        print(len(no_concept))
        for k, v in no_concept.most_common(20):
            print(k, v)
        with UnicodeWriter(self.dir / 'etc' / 'languages.csv') as w:
            w.writerow(['ID', 'Glottocode', 'Name', 'ISO639P3code', 'Latitude', 'Longitude', 'Alternative_Names'])
            w.writerows(languages)
        with UnicodeWriter(self.dir / 'etc' / 'concepts.csv') as w:
            w.writerow(['ID', 'GLOSS', 'CONCEPTICON_ID', 'WordListCategory'])
            w.writerows(concepts)
