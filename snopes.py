from bs4 import BeautifulSoup
import requests
import string


def snopes_scrape(search_term, num= 5):

    """
    Returns the top <num< matches from Snopes for a given keyword
    Args:
        search_term(string) : The search term that is queried on Snopes.
        num(int) L: Number of matches, maximum 18 for now. 

    Returns:
        list: The top num matches from Snopes.
    """
    if num>18:
        raise ValueError("The maximum value of number of results can be 18.")

    search_term = search_term.replace(" ", "+")
    r= requests.get('http://www.snopes.com/?s='+search_term+"&category=facts")
    soup = BeautifulSoup(r.text, 'lxml') 
    matches= [i.text for i in soup.find_all("h2", 
                {"class":'article-link-title'})[:num]]
    punctmap = str.maketrans('', '', string.punctuation+'‘’')
    matches = [s.translate(punctmap) for s in matches]
    data = soup.findAll('div',attrs={'class':'article-links-wrapper list-view'})[0]
    links = [i.get("href") for i in data.findAll('a')][:num]
    return zip(matches, links)

if __name__== "__main__":
    s = input("Enter search term: ")
    res = snopes_scrape(s)
    print (list(res))
