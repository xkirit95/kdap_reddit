from analysis import knol
import zstd
import os
import time
knol = knol()

#Extraction Methods
#Data Available between date 12/2005 to 12/2017
year = 2010
month = 12
destdir = "/home/xkirit0/kdap_reddit/kdap/"

#create directories to download knolml file into it.
if not os.path.isdir(destdir):
	os.makedirs(destdir)
if not os.path.isdir(destdir+"/KnolML"):
	os.makedirs(destdir+"/KnolML")
if not os.path.isdir(destdir+"/KnolML/posts"):
	os.makedirs(destdir+"/KnolML/posts")
	

file_name = os.path.join(destdir,"Dir/KnolML/Post1.knolml")
#s = time.time()
knol.download_dataset(sitename="reddit", destdir=destdir, datatype="QnA", year=year, month=month)
#e = time.time()
#print('Elapsed time: ', e-s)

'''
#frame_wise analysis on Reddit Knol-ML Dataset

frame = knol.frame(file_name)
for each in frame:
	print(each.get_editor())
	print(each.get_timestamp())
	print(each.get_score())
	print(each.get_text(clean=True))
	print(each.get_title())
	print(each.is_answer())
	print(each.is_question())
	print(each.is_comment())

'''
#Complex Analysis
########################
d = knol.get_comments_for_each_submissions(filename = file_name)
c = max(d, key=d.get)
print("\nSubmission with most comments:\n", c, "(number of comments = ", d[c]," )")

#########################
x = []
y = []
dc = {}
ind = 1
for f in os.listdir(os.path.join(destdir,"posts")):
	#print(f)
	dc.update(knol.get_submissions_per_subreddit(filename = os.path.join(os.path.join(destdir,"posts"), f)))
d = sorted(dc, key=dc.get, reverse=True)
#print(dc)
i = 0
for u in d:
	x.append(u)
	y.append(dc[u])
	i += 1
	if i == 10:
		break
knol.plot_(x, y, 'number of submissions','subreddit', title='Top 10 subreddits with highest number of submissions')

