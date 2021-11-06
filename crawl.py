import os
import sys
import csv
import json
import urllib.request
import argparse
import pprint

import humanize

from urllib.request import Request, urlopen
from collections import Counter

import yaml
import tqdm

'''
schema info:
https://code.google.com/archive/schema
should also be possible to get files
'''

SOURCE_KEYS = ['uncompressed_size', 'compressed_size', 'zip_file_size']
INFO_KEYS = [#'domain',
             'name', 'summary',
             #'description',
             'stars', 'license', 'contentLicense', 'labels', 'creationTime', 'repoType', 'subrepos', 'hasSource', 'ancestorRepo', 'logoName', 'imageUrl', 'movedTo']

OTHER_KEYS = ['total_sizes_by_language', 'percents_by_language', 'main_language', 'main_common_language']

MAX_SOURCE_PAGES = 20

COMMON_LANGUAGES = {'JavaScript', 'Python', 'Java', 'PHP', 'Ruby', 'C++', 'Go', 'C', 'C#', 'TypeScript', 'Shell', 'Swift', 'Scala', 'Objective-C', 'Rust', 'CoffeeScript', 'Haskell', 'Perl', 'Lua', 'Clojure'}

IGNORE_ERRORS = True

def size_counter_to_percentages(f_counter, limit=None):
    total = sum(f_counter.values())
    if total == 0:
        return {}
    return {k: round(100 * (v / total), 2) for k, v in f_counter.most_common(limit)}

def size_counter_to_human_readable(f_counter, limit=None):
    return {k: humanize.naturalsize(v) for k, v in f_counter.most_common(limit)}

def get_extension_to_language_map(languages_yaml_file, verbose=False):
    # this is not an exhaustive list, but it covered the cases that showed up when running this without the override check
    # 20 most popular languages in quarter 3 of 2016 (roughly when Google Code shut down, by https://madnight.github.io/githut/#/pull_requests/2016/3
    with open(languages_yaml_file, "r") as f:
        languages = yaml.load(f, Loader=yaml.FullLoader)
    extension_to_language = {}
    for language, language_info in sorted(languages.items()):
        for extension in language_info.get('extensions', []):
            if extension in extension_to_language:
                old_language = extension_to_language[extension]
                if old_language in COMMON_LANGUAGES:
                    if verbose:
                        print(f"{extension}: BLOCK  {old_language} (Common) | {language}")
                    continue
                    # do not update
                elif language in COMMON_LANGUAGES:
                    # update
                    if verbose:
                        print(f"{extension}: UPDATE {old_language} -> {language} (Common)")
                else:
                    # neither one is common; first-come-first-served
                    if verbose:
                        print(f"{extension}: BLOCK  {old_language} (First) | {language}")
                    continue
            extension_to_language[extension] = language
    return extension_to_language

EXTENSION_TO_LANGUAGE = get_extension_to_language_map("languages.yml")

def get_source_size(project_name):
    link = f"https://storage.googleapis.com/google-code-archive-source/v2/code.google.com/{project_name}/source-archive.zip"
    response = urllib.request.urlopen(link)
    return response.getheader('Content-Length')

def source_page_link(project_name, page_number=1):
    return f"https://storage.googleapis.com/google-code-archive/v2/code.google.com/{project_name}/source-page-{page_number}.json"


def get_source_info(project_name):
    """dict_keys(['Page', 'TotalPages', 'uncompressed_size', 'compressed_size', 'zip_file_size', 'num_entries', 'entries'])"""
    response = urllib.request.urlopen(source_page_link(project_name, page_number=1))
    data = json.loads(response.read().decode('utf-8'))
    return data

def get_project_info(project_name):
    """dict_keys(['domain', 'name', 'summary', 'description', 'stars', 'license', 'contentLicense', 'labels', 'creationTime', 'repoType', 'subrepos', 'hasSource', 'ancestorRepo', 'logoName', 'imageUrl', 'movedTo'])"""
    link = f"https://storage.googleapis.com/google-code-archive/v2/code.google.com/{project_name}/project.json"
    response = urllib.request.urlopen(link)
    return json.loads(response.read().decode('utf-8'))

def get_project_files(project_name, first_source_info=None):
    page = 1
    total_pages = None
    increment = 1
    while total_pages is None or page <= total_pages:
        if MAX_SOURCE_PAGES is not None and page > MAX_SOURCE_PAGES:
            break
        try:
            if first_source_info is not None and first_source_info.get('Page') == 1:
                data = first_source_info
            else:
                response = urllib.request.urlopen(source_page_link(project_name, page_number=1))
                data = json.loads(response.read().decode('utf-8'))
            total_pages = data['TotalPages']
            increment = max(int(total_pages / MAX_SOURCE_PAGES), 1)
            page += increment
            if data.get('entries') is not None:
                for entry in data['entries']:
                    if not entry['d'] and entry['s'] > 0:
                        # not a directory and size > 0
                        yield entry['f'], entry['s']
        except Exception as e:
            if not IGNORE_ERRORS:
                raise e
            print(f"{project_name}: {e}")
            break

def get_sizes_by_language(project_name, first_source_info=None):
    counts_by_language = Counter()
    non_empty_files = []
    empty_files = []
    for filename, size in get_project_files(project_name, first_source_info):
        if '/.hg/' in filename or '/.svn/' in filename or '/.git/' in filename:
            continue
        fname, ext = os.path.splitext(filename)
        if ext not in EXTENSION_TO_LANGUAGE:
            continue
        if size > 0:
            non_empty_files.append(filename)
        else:
            empty_files.append(filename)
        counts_by_language[EXTENSION_TO_LANGUAGE[ext]] += size
    return counts_by_language, non_empty_files, empty_files

def get_all_project_info(project_name):
    d = {}
    error = False
    try:
        project_info = get_project_info(project_name)
    except Exception as e:
        if not IGNORE_ERRORS:
            raise e
        print(f"{project_name}: {e}")
        error = True
        project_info = {}
    for key in INFO_KEYS:
        d[key] = project_info.get(key, '')
    try:
        source_info = get_source_info(project_name)
        sizes_by_language_counter, non_empty_files, empty_files = get_sizes_by_language(project_name, source_info)
        # iterator-based constructor to get ordering for json serialization
        sizes_by_language = {k: v for k,v in sizes_by_language_counter.most_common()}
        total = sum(sizes_by_language.values())
        if total == 0 or not bool(sizes_by_language):
            percents_by_language = {}
            main_language = None
            main_common_language = None
        else:
            percents_by_language = size_counter_to_percentages(sizes_by_language_counter)
            main_language = max(sizes_by_language.items(), key=lambda t: t[1])[0]
            main_common_language = max([(k, v) for k, v in sizes_by_language.items() if k in COMMON_LANGUAGES], 
                key=lambda t: t[1], default=(None, None))[0]
    except Exception as e:
        if not IGNORE_ERRORS:
            raise e
        print(f"{project_name}: {e}")
        error = True
        source_info = {}
        sizes_by_language = {}
        percents_by_language = {}
        main_language = None
        main_common_language = None
    for key in SOURCE_KEYS:
        d[key] = source_info.get(key, '')
        d['total_sizes_by_language'] = json.dumps(sizes_by_language)
        d['percents_by_language'] = json.dumps(percents_by_language)
        d['main_language'] = main_language
        d['main_common_language'] = main_common_language
    return d, error, sizes_by_language

if __name__ == "__main__":
#if False:
    print(' '.join(sys.argv))
    parser = argparse.ArgumentParser()
    parser.add_argument("in_fname")
    parser.add_argument("--out_fname", required=True)
    parser.add_argument("--start_index", type=int)
    parser.add_argument("--shard", type=int)
    parser.add_argument("--num_shards", type=int, default=5)

    args = parser.parse_args()
    pprint.pprint(vars(args))

    with open(args.in_fname, 'r') as f:
        repo_list = [l.strip() for l in f.readlines()]

    if args.shard is not None:
        start_ix = (len(repo_list) // args.num_shards) * args.shard
        end_ix = (len(repo_list) // args.num_shards) * (args.shard + 1)
    else:
        start_ix = None
        end_ix = None

    if args.start_index:
        start_ix = args.start_index

    if end_ix is not None:
        repo_list = repo_list[:end_ix+1]
    if start_ix is not None:
        repo_list = repo_list[start_ix:]

    print(f"scraping repos {start_ix} ({repo_list[0]}) -- {end_ix} ({repo_list[-1]}), inclusive")

    language_counts = Counter()
    star_counts = Counter()
    license_counts = Counter()

    total_sizes_by_language = Counter()
    total_open_source_sizes_by_language = Counter()

    records = []

    csv_file = open(args.out_fname, 'w')
    csv_writer = csv.DictWriter(csv_file, INFO_KEYS + SOURCE_KEYS + OTHER_KEYS)
    csv_writer.writeheader()

    repo_count = 0
    usable_repos = 0
    error_count = 0

    progress_bar = tqdm.tqdm(repo_list,ncols=120)

    for repo in progress_bar:
        record, error, sizes_by_language = get_all_project_info(repo)
        records.append(record)
        csv_writer.writerows([record])

        if error:
            error_count += 1

        main_common_language = record.get('main_common_language')
        stars = record.get('stars')
        license = record.get('license')

        language_counts[main_common_language] += 1
        star_counts[stars] += 1
        license_counts[license] += 1
        total_sizes_by_language += sizes_by_language

        open_source = license in {'mit', 'asf20', 'bsd'}

        if open_source:
            total_open_source_sizes_by_language += sizes_by_language

        is_usable = open_source and (main_common_language == 'Python')
        if is_usable:
            usable_repos += 1
        repo_count += 1

        # print(f"{repo_count:_}\t{record}")

        if repo_count % 10 == 0:
            progress_bar.set_description(f'{usable_repos/repo_count:.2f} {repo}')

        if (repo_count) % 100 == 0:
            print("**printing stats**")
            print(f"{usable_repos:_} / {repo_count:_} usable repos\t{error_count} errors")
            print("most common language counts:")
            print(language_counts.most_common())
            print("license counts:")
            print(license_counts.most_common())
            print("language amounts (all repos):")
            print(size_counter_to_human_readable(total_sizes_by_language, limit=20))
            print("language amounts (open-source repos):")
            print(size_counter_to_human_readable(total_open_source_sizes_by_language, limit=20))
            print("language fractions (all repos):")
            print(size_counter_to_percentages(total_sizes_by_language, limit=20))
            print("language fractions (open-source repos):")
            print(size_counter_to_percentages(total_open_source_sizes_by_language, limit=20))
            print()
            csv_file.flush()
