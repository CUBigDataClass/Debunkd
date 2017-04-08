import sys

from debunkr.snopes.snopes import snopes_scrape
from debunkr.gnip.gnipreader import GnipData

def main():
	query = "hillary"
	results = snopes_scrape(query)
	search_term = (list(results)[0][0])
	print(search_term)
	a = GnipData(100, 10,'201601300000', '201612310000')
	a.fetchTweets(search_term)


if __name__ == "__main__":
	main()