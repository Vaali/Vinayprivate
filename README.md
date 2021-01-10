# Objective
The main aim of the project is to map songs from discogs to youtube and create a data repository which can be indexed and searched.


# Steps

- Unique Songs: Get unique songs from the list of songs from discogs database. Run the program getsongslist.py . It expects the data directory from discogs. Set IsCrawlingYoutube to 0 in config file.
- Crawl Youtube: Use the songslist from previous step to crawl youtube to match videos with songs. Run python launchartistscrawling.py. And set IsYoutudeApi based on requirement.
- Generatexmls: Used to generate xmls . Run generatexmls_discogs.py .
