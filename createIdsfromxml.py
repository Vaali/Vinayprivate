from lxml import etree
def decodexml(s):
 	s = s.replace("0000", "&")
	s = s.replace("_", " ")	   
	s = s.replace("1111", "/")
	s = s.replace("2222", "-")
	s = s.replace("3333", "\\")	   
	s = s.replace("4444", ".")
	s = s.replace("5555", "'")
	s = s.replace("6666", "(")	   
	s = s.replace("7777", ")")
	s = s.replace("8888", "!")
   	return s

def CalculateIdsforGenres(node,currentValue):
	#genreMat[decodexml(node.tag.lower())] = currentGenre
	current_list = []
	
	for child in node.iterchildren("*"):
		#print child.tag
		current_list.append(decodexml(child.tag.lower()))
	current_list.sort()
	#print current_list
	if(current_list == None):
		return
	for child in current_list:
		currentValue = currentValue+1
		genreMat[child] = currentValue
		#print currentGenre
	for child in node.iterchildren("*"):
		CalculateIdsforGenres(child,currentValue)

def CalculateIds():
	doc1 = etree.parse(open('Rock.xml'))
	currentGenre = 1
	children = doc1.getroot().iterchildren("*")
	print children
	for child in doc1.getroot().iterchildren("*"):
		print child.tag.lower()
		print currentGenre
		genreMat[decodexml(child.tag.lower())] = currentGenre
		CalculateIdsforGenres(child,currentGenre)
		currentGenre = currentGenre + 100

genreMat = {}
CalculateIds()
print genreMat



