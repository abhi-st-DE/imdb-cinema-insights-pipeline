# IMDb Non-Commercial Datasets

Subsets of IMDb data are available for access to customers for personal and non-commercial use. You can hold local copies of this data, and it is subject to our terms and conditions. Please refer to the Non-Commercial Licensing and copyright/license and verify compliance.

---

## Notice

As of March 18, 2024 the datasets on this page are backed by a new data source. There has been no change in location or schema, but if you encounter issues with the datasets following the March 18th update, please contact [imdb-data-interest@imdb.com](mailto:imdb-data-interest@imdb.com).

---

## Data Location

The dataset files can be accessed and downloaded from [https://datasets.imdbws.com/](https://datasets.imdbws.com/). The data is refreshed daily.

---

## IMDb Dataset Details

Each dataset is contained in a gzipped, tab-separated-values (TSV) formatted file in the UTF-8 character set. The first line in each file contains headers that describe what is in each column. A `\N` is used to denote that a particular field is missing or null for that title/name. The available datasets are as follows:

---

### `title.akas.tsv.gz`

| Field | Type | Description |
|-------|------|-------------|
| `titleId` | string | A `tconst`, an alphanumeric unique identifier of the title. |
| `ordering` | integer | A number to uniquely identify rows for a given `titleId`. |
| `title` | string | The localized title. |
| `region` | string | The region for this version of the title. |
| `language` | string | The language of the title. |
| `types` | array | Enumerated set of attributes for this alternative title. One or more of: `"alternative"`, `"dvd"`, `"festival"`, `"tv"`, `"video"`, `"working"`, `"original"`, `"imdbDisplay"`. New values may be added in the future without warning. |
| `attributes` | array | Additional terms to describe this alternative title, not enumerated. |
| `isOriginalTitle` | boolean | `0`: not original title; `1`: original title. |

---

### `title.basics.tsv.gz`

| Field | Type | Description |
|-------|------|-------------|
| `tconst` | string | Alphanumeric unique identifier of the title. |
| `titleType` | string | The type/format of the title (e.g. `movie`, `short`, `tvseries`, `tvepisode`, `video`, etc.). |
| `primaryTitle` | string | The more popular title / the title used by the filmmakers on promotional materials at the point of release. |
| `originalTitle` | string | Original title, in the original language. |
| `isAdult` | boolean | `0`: non-adult title; `1`: adult title. |
| `startYear` | YYYY | Represents the release year of a title. In the case of TV Series, it is the series start year. |
| `endYear` | YYYY | TV Series end year. `\N` for all other title types. |
| `runtimeMinutes` | integer | Primary runtime of the title, in minutes. |
| `genres` | string array | Includes up to three genres associated with the title. |

---

### `title.crew.tsv.gz`

| Field | Type | Description |
|-------|------|-------------|
| `tconst` | string | Alphanumeric unique identifier of the title. |
| `directors` | array of nconsts | Director(s) of the given title. |
| `writers` | array of nconsts | Writer(s) of the given title. |

---

### `title.episode.tsv.gz`

| Field | Type | Description |
|-------|------|-------------|
| `tconst` | string | Alphanumeric identifier of the episode. |
| `parentTconst` | string | Alphanumeric identifier of the parent TV Series. |
| `seasonNumber` | integer | Season number the episode belongs to. |
| `episodeNumber` | integer | Episode number of the `tconst` in the TV series. |

---

### `title.principals.tsv.gz`

| Field | Type | Description |
|-------|------|-------------|
| `tconst` | string | Alphanumeric unique identifier of the title. |
| `ordering` | integer | A number to uniquely identify rows for a given `titleId`. |
| `nconst` | string | Alphanumeric unique identifier of the name/person. |
| `category` | string | The category of job that person was in. |
| `job` | string | The specific job title if applicable, else `\N`. |
| `characters` | string | The name of the character played if applicable, else `\N`. |

---

### `title.ratings.tsv.gz`

| Field | Type | Description |
|-------|------|-------------|
| `tconst` | string | Alphanumeric unique identifier of the title. |
| `averageRating` | float | Weighted average of all the individual user ratings. |
| `numVotes` | integer | Number of votes the title has received. |

---

### `name.basics.tsv.gz`

| Field | Type | Description |
|-------|------|-------------|
| `nconst` | string | Alphanumeric unique identifier of the name/person. |
| `primaryName` | string | Name by which the person is most often credited. |
| `birthYear` | YYYY | Birth year. |
| `deathYear` | YYYY | Death year if applicable, else `\N`. |
| `primaryProfession` | array of strings | The top-3 professions of the person. |
| `knownForTitles` | array of tconsts | Titles the person is known for. |
