# coding=utf-8
"""
https://diacl.ht.lu.se/Content/documents/DiACL-lexicology.pdf
"""
from __future__ import unicode_literals, print_function

import re
from collections import defaultdict, Counter, OrderedDict
from itertools import groupby
import gzip
from json import dumps, loads

from tqdm import tqdm
import attr
from clldutils.path import Path, remove
from clldutils.jsonlib import load
from csvw.dsv import UnicodeWriter
from pycldf.sources import Source
from pylexibank.dataset import Lexeme, Cognate, Concept, Dataset as BaseDataset, Language


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

    def _iter_langs(self):
        for lang in self.raw.read_xml('languages.xml', wrap=False).findall('.//language'):
            yield lang.get('language-id'), lang

    def cmd_download(self, **kw):
        """
        Download files to the raw/ directory. You can use helpers methods of `self.raw`, e.g.

        >>> self.raw.download(url, fname)
        """
        #https://diacl.ht.lu.se/GeoJson/GeographicalPresence/24
        print('Download wordlists ...')
        self.raw.download(self._url('/WordList/Index'), 'wordlists.html', skip_if_exists=True)
        for p in re.findall('/Xml/WordListWithLanguageLexemes/[0-9]+', self.raw.read('wordlists.html')):
            self.raw.download(self._url(p), 'wl{0}.xml'.format(p.split('/')[-1]), skip_if_exists=True)
        print('... done')
        print('Download languages ...')
        self.raw.download(self._url('/Xml/AllLanguages'), 'languages.xml')
        for id_, _ in tqdm(self._iter_langs()):
            target = 'l{0}'.format(id_)
            self.raw.download(self._url('/Xml/SingleLanguageWithLexemes/{0}'.format(id_)), target)
        print('... done')
        print('Download etymologies ...')
        for id_, _ in tqdm(self._iter_langs()):
            target = 'l{0}'.format(id_)
            for lex in self.raw.read_xml(target, wrap=False).findall('.//lexeme'):
                lid = lex.get('lexeme-id')
                target = '{0}.json'.format(lid)
                self.raw.download(self._url('/Json/EtymologyTree/{0}'.format(lid)), target, skip_if_exists=True)
        print('... done')

        etymologytrees = OrderedDict()
        for p in sorted(self.raw.glob('*.json'), key=lambda p: int(p.stem)):
            etymologytrees[int(p.stem)] = load(p)['etymologies']
            remove(p)

        with gzip.GzipFile(str(self.raw.joinpath('etymology.json.gz')), 'w') as fp:
            fp.write(dumps(etymologytrees).encode('utf8'))

        self.raw.download(self._url('/Json/LanguageTree'), 'LanguageTree.json')

    def split_forms(self, item, value):
        # We only take the first form, since the proliferation of variants seems to be rather
        # unprincipled, otherwise.
        return [value.split(',')[0]]

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

        #    <word-list-category word-list-category-id="100">
        #        <name>Swadesh 100</name>
        #        <word-list-items>
        #            <word-list-item word-list-item-id="1001">
        #                <name>all (of a number)</name>
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

            for wordlist in wl.findall('.//word-list'):
                for wlc in wordlist.findall('.//word-list-category'):
                    for wli in wlc.findall('.//word-list-item'):
                        wliid = int(wli.get('word-list-item-id'))
                        if concept_map[wliid]:
                            # Only keep mapped concepts:
                            for lc in wli.findall('.//lexeme-connection'):
                                lid = int(_get_text(lc, 'lexeme-id'))
                                lexemes[lid]['concepts'].add(concept_map[wliid])

        # Compute cognate sets (or etymologies) as union of all lexemes ocurring together in any
        # EtymologyTree:
        cogsets = []
        with gzip.GzipFile(str(self.raw.joinpath('etymology.json.gz')), 'r') as fp:
            # we have to cluster using etymologies where FkReliabilityId < 2
            # we also assume cognacy to be transitive
            for etymon in loads(fp.read().decode('utf8')).values():
                for e in etymon.values():
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

        # remove duplicates:
        cogsets = set([tuple(sorted(c)) for c in cogsets])

        # Augment sets of lexeme ids with associated concepts:
        cogsets = [set(
            (l, frozenset(lexemes[l]['concepts']))
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
            for cid, gloss in concepts.items():
                if cid:
                    ds.add_concept(ID=cid, Name=gloss, Concepticon_ID=cid)

            for src in sorted(sources.values(), key=lambda s: s['key']):
                ds.add_sources(src)

            for lid, lang in sorted(languages.items()):
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
    res['concepts'] = set()
    res['meaning'] = _get_text(l.find('semantics'), 'meaning')
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


# wl1.xml contains Swadesh 100 and 200
"""
<?xml version="1.0" encoding="utf-8"?>
<word-list-with-lexemes>
  <diacl-timestamp unix-time="1539679719" iso-8601="2018-10-16T08:48:39" retrieved-from="https://diacl.ht.lu.se/" />
  <language-collection>
    <languages>
      <language language-id="100">
        <name>Aikanã</name>
        <alternative-names>Aikaná, Corumbiara, Huari, Kasupá, Kolumbiara, Masaká, Mundé, Tubarão, Uari, Wari</alternative-names>
        <note />
        <native-speakers />
        <iso-693-3>tba</iso-693-3>
        <focal-point>
          <wgs84-latitude>-11.808008</wgs84-latitude>
          <wgs84-longitude>-61.788401</wgs84-longitude>
        </focal-point>
        <time-frame><from>1750</from><until>2000</until></time-frame>
        <focus-area-id>150</focus-area-id>
        <language-area-id>200</language-area-id>
        <reliability-id>10</reliability-id>
        <lexemes>
          <lexeme lexeme-id="21615">
            <language-id>100</language-id>
            <form-transcription>yøtє, yute</form-transcription>
            <form-transliteration />
            <form-ipa />
            <grammatical-data />
            <note />
            <semantics>
              <meaning>star</meaning>
              <note />
            </semantics>
            <sources>
              <source>
                <source-id>3088</source-id>
                <location-within-source>165</location-within-source>
                <note />
              </source>
              <source><source-id>3102</source-id><location-within-source /><note /></source>
              </sources>
  <word-list word-list-id="1">
    <name>Swadesh</name>
    <description />
    <focus-area-id />
    <word-list-categories>
      <word-list-category word-list-category-id="100">
        <name>Swadesh 100</name>
        <word-list-items>
          <word-list-item word-list-item-id="1001">
            <name>all (of a number)</name>
            <note />
            <lexeme-connections>
              <lexeme-connection lexeme-connection-id="21744"><lexeme-id>5734</lexeme-id></lexeme-connection>
              <lexeme-connection lexeme-connection-id="21745"><lexeme-id>5930</lexeme-id></lexeme-connection><lexeme-connection l


  <sources>
              <source source-id="666">
              <citation-key>Verify (2015)</citation-key>
              <full-citation>Needs to be verified after data migration.</full-citation>
              <note>This is a placeholder for data points during data migration that did not have a source listed, but that can be identified after migration.</note>
              <type value="0">Literature</type>
            </source>
"""


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
