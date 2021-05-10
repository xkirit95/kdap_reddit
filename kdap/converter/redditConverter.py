from datetime import datetime
from dateutil import tz
import numpy as np
import os.path
import requests
import io
import json
import xml.etree.ElementTree as ET
import textwrap
import html
import os.path
from datetime import datetime
from subprocess import call
import errno
import os
import sys
import pandas as pd
from itertools import product
import zstd
import time
import re


class redditConverter(object):
	def __init__(self, destdir, dtype):
		self.dest_dir = destdir
		#s = time.time()
		#self.reddit_knolml_converter_bruteforce()
		#e = time.time()
		#print('Elapsed Time using Brute_Force: ', e-s)
		#s = time.time()
		self.reddit_knolml_converter_Hash()
		#self.remove_tmp_files()
		#e = time.time()
		#print('Elapsed Time using Hashing: ', e-s)
	
	def remove_tmp_files(self):
		tmp_dir = os.path.join(self.dest_dir,'temp')
		for files in os.listdir(tmp_dir):
			os.remove(os.path.join(tmp_dir,files))
	def reddit_knolml_converter_bruteforce(self, *args, **kwargs):
		chsize = 1000
		post_id = 1

		#remove existing files in directory posts
		if not os.path.isdir(self.dest_dir+'/posts'):
			os.makedirs(self.dest_dir+ 'posts')
		post_dir = self.dest_dir+'/posts/'

		for files in os.listdir(post_dir):
			os.remove(os.path.join(post_dir,files))
			
		file_name = self.dest_dir + '/posts/Post1.knolml' 
		sub_loc = self.dest_dir + 'submissions'
		comm_loc = self.dest_dir + 'comments'
		sub_f = pd.read_json(sub_loc, lines = True, chunksize = chsize)
		instance_id = 1
		for sub_df in sub_f:
			f = open(file_name, 'a+',encoding = 'utf-8')
			rjson = sub_df.to_json(orient = 'records')
			r = json.loads(rjson)
			
			f.write("<?xml version='1.0' encoding='utf-8'?>\n")
			f.write("<KnolML>\n")
			t2 = '\t\t'
			t3 = '\t\t\t'
			t4 = '\t\t\t\t'
			t5 = '\t\t\t\t\t'
			f.write("\t<KnowledgeData "+"Type="+'"'+"QA/text"+'"'+">\n")
			
			for elem in r:
				f.write(t2+'<Instance Id= '+'"'+str(instance_id)+'"'+' InstanceType= "Question" >'+"\n")
				title = str(elem['title'])
				title = str(title.encode("ascii", "ignore"))
				title = title[2:].replace('\\x1', '')
				f.write(t3+'<Title>'+str(title)+'</Title>'+"\n")
				f.write(t3+"<TimeStamp>"+"\n")
				f.write(t4+"<CreationUTC>"+str(elem['created_utc'])+"</CreationUTC> "+"\n")
				f.write(t3+"</TimeStamp>"+"\n")
				
				f.write(t3+"<Contributors>"+"\n")
				if not 'author' in elem:
					 elem['author'] = ""
				f.write(t4+"<OwnerUserName>"+str(elem['author'])+"</OwnerUserName> "+"\n")
				f.write(t3+"</Contributors>"+"\n")
				
				f.write(t3+"<SubmissionDetails>\n")
				if not 'subreddit_id' in elem:
					elem['subreddit_id'] = "";
				if not 'subreddit' in elem:
					elem['subreddit'] = "";
				f.write(t4+'<Subreddit Id = '+'"'+str(elem['subreddit_id'])+'"'+' Name = '+'"'+ str(elem['subreddit'])+'"'+' />\n')
				if not 'name' in elem:
					elem['name'] = "";
				f.write(t4+'<Submission Name = '+'"'+str(elem['name'])+'"'+" ")
				if not 'id' in elem:
					elem['id'] = ""
				f.write('Id = '+'"'+str(elem['id'])+'"'+" ")
				if not 'url' in elem:
					elem['url'] = "";
				else:
					s1 = str(elem['url'])
					s1 = str(s1.encode("ascii", "ignore"))
					s1 = s1[2:].replace('\\x1', '')
					elem['url'] = s1.replace('"','')
				f.write('Url = '+'"'+elem['url']+'"'+" ")
				if not 'permalink' in elem:
					elem['permalink'] = "";
				else:
					s1 = str(elem['permalink'])
					s1 = str(s1.encode("ascii", "ignore"))
					s1 = s1[2:].replace('\\x1', '')
					elem['permalink'] = s1.replace('"','')
				f.write('Permalink = '+'"'+elem['permalink']+'"'+" ")
				if not 'num_comments' in elem:
					elem[num_commnets] = 0;
				f.write('num_comments = '+'"'+str(elem['num_comments'])+'"'+" ")
				if not 'domain' in elem:
					elem['domain'] = "";
				else:
					s1 = str(elem['domain'])
					s1 = str(s1.encode("ascii", "ignore"))
					s1 = s1[2:].replace('\\x1', '')
					elem['domain'] = s1.replace('"','')
				f.write('Domain = '+'"'+elem['domain']+'"'+" ")
				if not 'is_self' in elem:
					elem['is_self'] = ""
				f.write('Is_self = '+'"'+str(elem['is_self'])+'"'+" ")
				if not 'over_18' in elem:
					elem['over_18'] = ""
				f.write('Over_18 = '+'"'+str(elem['over_18'])+'"'+" ")
				if not 'hidden' in elem:
					elem['hidden'] = ""
				f.write('Hidden = '+'"'+str(elem['hidden'])+'"'+" ")
				if not 'thumbnail' in elem:
					elem['thumbnail'] = ""
				f.write('Thumbnail = '+'"'+str(elem['thumbnail'])+'"'+"/>\n")
				f.write(t3+"</SubmissionDetails>\n")
				f.write(t3+"<Body>"+"\n")
				f.write(t4+"<Text>"+"\n")
				s = str(elem['selftext'])
				s = str(s.encode("ascii", "ignore"))
				s = s[2:].replace('\\x1', '')
				s = s.split('\n')
				for each in s:
					f.write(t5+each+"\n")
				f.write(t4+"</Text> "+"\n")
				f.write(t3+"</Body>"+"\n")  
				
				f.write(t3+"<Credit>"+"\n")
				f.write(t4+"<Score>"+str(elem['score'] if 'score' in elem else 0)+"</Score> "+"\n")
				f.write(t3+"</Credit>"+"\n")   
				
				f.write(t2+'</Instance>'+"\n")
				
				answer_id = 0
				comm_f = pd.read_json(comm_loc, lines = True, chunksize = chsize)
				
				for comm_df in comm_f:
					cjson = comm_df.to_json(orient = 'records')
					c = json.loads(cjson)
					for line in c:
						pid = line['parent_id'].split('_')
						if pid[1] == elem['id']:
							answer_id += 1
							f.write(t2+'<Instance Id= '+'"'+str(answer_id)+'"'+' InstanceType= "Answer" ParentId = '+'"'+ elem['id'] +'">'+"\n")
					
							f.write(t3+"<TimeStamp>"+"\n")
							f.write(t4+"<CreationUTC>"+str(line['created_utc'])+"</CreationUTC> "+"\n")
							f.write(t3+"</TimeStamp>"+"\n")
							
							f.write(t3+"<Contributors>"+"\n")
							if not 'author' in line:
								 line['author'] = ""
							if not 'id' in line:
								line['id'] = ""
							f.write(t4+"<OwnerUserId>"+line['id']+"</OwnerUserId> "+"\n")
							f.write(t4+"<OwnerUserName>"+line['author']+"</OwnerUserName> "+"\n")
							f.write(t3+"</Contributors>"+"\n")
							
							f.write(t3+"<CommentDetails>\n")
							if not 'subreddit_id' in line:
								line['subreddit_id'] = ""
							if not 'subreddit' in line:
								line['subreddit'] = ""
							f.write(t4+'<Subreddit Id = '+'"'+line['subreddit_id']+'"'+' Name = '+'"'+ line['subreddit']+'"'+' />\n')
							if not 'link_id' in line:
								line['link_id'] = ""
							f.write(t4+'<Comment link_id = '+'"'+str(line['link_id'])+'"'+" ")
							if not 'edited' in line:
								line['edited'] = ""
							f.write('edited = '+'"'+str(line['edited'])+'"'+" ")
							if not 'stickied' in line:
								line['stickied'] = ""
							f.write('stickied = '+'"'+str(line['stickied'])+'"'+" ")
							if not 'controversiality' in line:
								line['controversiality'] = ""
							f.write('controversiality = '+'"'+str(line['controversiality']) +'"'+" ")
							if not 'distinguished' in line:
								line['distinguished'] = ""
							f.write('distinguished = '+'"'+str(line['distinguished'])+'"'+" ")
							if not 'retrieved_on' in line:
								line['retrieved_on']=""
							f.write('retrieved_on = '+'"'+str(line['retrieved_on'])+'"'+" ")
							if not 'gilded' in line:
								line['gilded'] = ""
							f.write('gilded = '+'"'+str(line['gilded'])+'"'+" ")
							
							if not 'author_flair_text' in line or not line['author_flair_text']:
								line['author_flair_text'] = ""
							else:
								line['author_flair_text'] = str(line['author_flair_text']).replace('"','')
							f.write('author_flair_text = '+'"'+str(line['author_flair_text'])+'"'+" ")
							
							if not 'author_flair_css_class' in line or not line['author_flair_css_class']:
								line['author_flair_css_class'] = ""
							else:
								line['author_flair_css_class'] = str(line['author_flair_css_class']).replace('"','')
							f.write('author_flair_css_class = '+'"'+str(line['author_flair_css_class'])+'"'+" ")
							f.write(' />\n')
							f.write(t3+"</CommentDetails>\n")
							f.write(t3+"<Body>"+"\n")
							f.write(t4+"<Text>"+"\n")
							s = line['body']
							s = str(s.encode("ascii", "ignore"))
							s = s[2:].replace('\\x1', '')
							s = s.split('\n')
							for each in s:
								f.write(t5+each+"\n")
							f.write(t4+"</Text> "+"\n")
							f.write(t3+"</Body>"+"\n")  
							
							f.write(t3+"<Credit>"+"\n")
							f.write(t4+"<Score>"+str(elem['score'] if 'score' in elem else 0)+"</Score> "+"\n")
							f.write(t3+"</Credit>"+"\n")   
							
							f.write(t2+'</Instance>'+"\n")
				instance_id += 1
			f.write('\t'+'</KnowledgeData>'+"\n")
			f.write('</KnolML>\n')

			post_id += 1
			f.close()
			file_name = self.dest_dir + '/posts/Post'+str(post_id)+'.knolml' 
		
	def reddit_knolml_converter_Hash(self, *args, **kwargs):
		chsize = 1000
		post_id = 1
		#self.create_tmp_files(chsize)   
		tmp_dir = os.path.join(self.dest_dir,'temp')
		

		#remove existing files in directory posts
		if not os.path.isdir(self.dest_dir+'/posts'):
			os.makedirs(self.dest_dir+ 'posts')
		post_dir = self.dest_dir+'/posts/'

		for files in os.listdir(post_dir):
			os.remove(os.path.join(post_dir,files))
			
		file_name = self.dest_dir + '/posts/Post1.knolml' 
		sub_loc = self.dest_dir + 'submissions'
		sub_f = pd.read_json(sub_loc, lines = True, chunksize = chsize)
		instance_id = 1
		
		for sub_df in sub_f:
			f = open(file_name, 'a+',encoding = 'utf-8')
			rjson = sub_df.to_json(orient = 'records')
			r = json.loads(rjson)
			
			f.write("<?xml version='1.0' encoding='utf-8'?>\n")
			f.write("<KnolML>\n")
			t2 = '\t\t'
			t3 = '\t\t\t'
			t4 = '\t\t\t\t'
			t5 = '\t\t\t\t\t'
			f.write("\t<KnowledgeData "+"Type="+'"'+"QA/text"+'"'+">\n")
			
			for elem in r:
				#print(elem)
				f.write(t2+'<Instance Id= '+'"'+str(instance_id)+'"'+' InstanceType= "Question" >'+"\n")
				title = str(elem['title'])
				title = str(title.encode("ascii", "ignore"))
				title = title[2:].replace('\\x1', '')
				f.write(t3+'<Title>'+str(title)+'</Title>'+"\n")
				f.write(t3+"<TimeStamp>"+"\n")
				f.write(t4+"<CreationUTC>"+str(elem['created_utc'])+"</CreationUTC> "+"\n")
				f.write(t3+"</TimeStamp>"+"\n")
				
				f.write(t3+"<Contributors>"+"\n")
				if not 'author' in elem:
					 elem['author'] = ""
				f.write(t4+"<OwnerUserName>"+str(elem['author'])+"</OwnerUserName> "+"\n")
				f.write(t3+"</Contributors>"+"\n")
				
				f.write(t3+"<SubmissionDetails>\n")
				if not 'subreddit_id' in elem:
					elem['subreddit_id'] = "";
				if not 'subreddit' in elem:
					elem['subreddit'] = "";
				f.write(t4+'<Subreddit Id = '+'"'+str(elem['subreddit_id'])+'"'+' Name = '+'"'+ str(elem['subreddit'])+'"'+' />\n')
				if not 'name' in elem:
					elem['name'] = "";
				f.write(t4+'<Submission Name = '+'"'+str(elem['name'])+'"'+" ")
				if not 'id' in elem:
					elem['id'] = ""
				f.write('Id = '+'"'+str(elem['id'])+'"'+" ")
				if not 'url' in elem:
					elem['url'] = "";
				else:
					s1 = str(elem['url'])
					s1 = str(s1.encode("ascii", "ignore"))
					s1 = s1[2:].replace('\\x1', '')
					elem['url'] = s1.replace('"','')
				f.write('Url = '+'"'+elem['url']+'"'+" ")
				if not 'permalink' in elem:
					elem['permalink'] = "";
				else:
					s1 = str(elem['permalink'])
					s1 = str(s1.encode("ascii", "ignore"))
					s1 = s1[2:].replace('\\x1', '')
					elem['permalink'] = s1.replace('"','')
				f.write('Permalink = '+'"'+elem['permalink']+'"'+" ")
				if not 'num_comments' in elem:
					elem[num_commnets] = 0;
				f.write('num_comments = '+'"'+str(elem['num_comments'])+'"'+" ")
				if not 'domain' in elem:
					elem['domain'] = "";
				else:
					s1 = str(elem['domain'])
					s1 = str(s1.encode("ascii", "ignore"))
					s1 = s1[2:].replace('\\x1', '')
					elem['domain'] = s1.replace('"','')
				f.write('Domain = '+'"'+elem['domain']+'"'+" ")
				if not 'is_self' in elem:
					elem['is_self'] = ""
				f.write('Is_self = '+'"'+str(elem['is_self'])+'"'+" ")
				if not 'over_18' in elem:
					elem['over_18'] = ""
				f.write('Over_18 = '+'"'+str(elem['over_18'])+'"'+" ")
				if not 'hidden' in elem:
					elem['hidden'] = ""
				f.write('Hidden = '+'"'+str(elem['hidden'])+'"'+" ")
				if not 'thumbnail' in elem:
					elem['thumbnail'] = ""
				f.write('Thumbnail = '+'"'+str(elem['thumbnail'])+'"'+"/>\n")
				f.write(t3+"</SubmissionDetails>\n")
				f.write(t3+"<Body>"+"\n")
				f.write(t4+"<Text>"+"\n")
				s = str(elem['selftext'])
				s = str(s.encode("ascii", "ignore"))
				s = s[2:].replace('\\x1', '')
				s = s.split('\n')
				for each in s:
					f.write(t5+each+"\n")
				f.write(t4+"</Text> "+"\n")
				f.write(t3+"</Body>"+"\n")  
				
				f.write(t3+"<Credit>"+"\n")
				f.write(t4+"<Score>"+str(elem['score'] if 'score' in elem else 0)+"</Score> "+"\n")
				f.write(t3+"</Credit>"+"\n")   
				
				f.write(t2+'</Instance>'+"\n")
				self.write_comment_history(f, tmp_dir, elem['id'], chsize)
				instance_id += 1
			f.write('\t'+'</KnowledgeData>'+"\n")
			f.write('</KnolML>\n')
			
			post_id += 1
			f.close()
			file_name = os.path.join(self.dest_dir, 'posts')+'/Post'+str(post_id)+'.knolml'
	
	def write_comment_history(self, f, tmp_dir, elem_id, chsize):
		answer_id = 0
		t2 = '\t\t'
		t3 = '\t\t\t'
		t4 = '\t\t\t\t'
		t5 = '\t\t\t\t\t'
		#print(tmp_dir, elem_id)
		comm_file = os.path.join(tmp_dir,elem_id)
		comm_file = comm_file[:3]
		#print(comm_file)
		if not os.path.isfile(comm_file):
			#print("FILE DNE")
			return
		comm_f = pd.read_json(comm_file, lines = True, chunksize = chsize)
		for comm_df in comm_f:
			#print(type(comm_df))
			cjson = comm_df.to_json(orient = 'records')
			c = json.loads(cjson)
			for line in c:
				pid = line['parent_id'][3:]
				#print('pid:', pid, ' elem_id:', elem_id)
				if pid == elem_id:
					answer_id += 1
					f.write(t2+'<Instance Id= '+'"'+str(answer_id)+'"'+' InstanceType= "Answer" ParentId = '+'"'+ elem_id +'">'+"\n")
			
					f.write(t3+"<TimeStamp>"+"\n")
					f.write(t4+"<CreationUTC>"+str(line['created_utc'])+"</CreationUTC> "+"\n")
					f.write(t3+"</TimeStamp>"+"\n")
					
					f.write(t3+"<Contributors>"+"\n")
					if not 'author' in line:
						 line['author'] = ""
					if not 'id' in line:
						line['id'] = ""
					f.write(t4+"<OwnerUserId>"+str(line['id'])+"</OwnerUserId> "+"\n")
					f.write(t4+"<OwnerUserName>"+str(line['author'])+"</OwnerUserName> "+"\n")
					f.write(t3+"</Contributors>"+"\n")
					
					f.write(t3+"<CommentDetails>\n")
					if not 'subreddit_id' in line:
						line['subreddit_id'] = ""
					if not 'subreddit' in line:
						line['subreddit'] = ""
					f.write(t4+'<Subreddit Id = '+'"'+str(line['subreddit_id'])+'"'+' Name = '+'"'+ str(line['subreddit'])+'"'+' />\n')
					if not 'link_id' in line:
						line['link_id'] = ""
					f.write(t4+'<Comment link_id = '+'"'+str(line['link_id'])+'"'+" ")
					if not 'edited' in line:
						line['edited'] = ""
					f.write('edited = '+'"'+str(line['edited'])+'"'+" ")
					if not 'stickied' in line:
						line['stickied'] = ""
					f.write('stickied = '+'"'+str(line['stickied'])+'"'+" ")
					if not 'controversiality' in line:
						line['controversiality'] = ""
					f.write('controversiality = '+'"'+str(line['controversiality']) +'"'+" ")
					if not 'distinguished' in line:
						line['distinguished'] = ""
					f.write('distinguished = '+'"'+str(line['distinguished'])+'"'+" ")
					if not 'retrieved_on' in line:
						line['retrieved_on']=""
					f.write('retrieved_on = '+'"'+str(line['retrieved_on'])+'"'+" ")
					if not 'gilded' in line:
						line['gilded'] = ""
					f.write('gilded = '+'"'+str(line['gilded'])+'"'+" ")
					
					if not 'author_flair_text' in line or not line['author_flair_text']:
						line['author_flair_text'] = ""
					else:
						line['author_flair_text'] = str(line['author_flair_text']).replace('"','')
					f.write('author_flair_text = '+'"'+str(line['author_flair_text'])+'"'+" ")
					
					if not 'author_flair_css_class' in line or not line['author_flair_css_class']:
						line['author_flair_css_class'] = ""
					else:
						line['author_flair_css_class'] = str(line['author_flair_css_class']).replace('"','')
					f.write('author_flair_css_class = '+'"'+str(line['author_flair_css_class'])+'"'+" ")
					f.write(' />\n')
					f.write(t3+"</CommentDetails>\n")
					f.write(t3+"<Body>"+"\n")
					f.write(t4+"<Text>"+"\n")
					s = str(line['body'])
					s = str(s.encode("ascii", "ignore"))
					s = s[2:].replace('\\x1', '')
					s = s.split('\n')
					for each in s:
						f.write(t5+each+"\n")
					f.write(t4+"</Text> "+"\n")
					f.write(t3+"</Body>"+"\n")  
					
					f.write(t3+"<Credit>"+"\n")
					f.write(t4+"<Score>"+str(line['score'] if 'score' in line else 0)+"</Score> "+"\n")
					f.write(t3+"</Credit>"+"\n")   
					
					f.write(t2+'</Instance>'+"\n")
		
	def create_tmp_files(self, chsize):
		tmp_id = 1

		#remove existing files in directory posts
		if not os.path.isdir(self.dest_dir+'/temp'):
			os.makedirs(self.dest_dir+ 'temp')
		tmp_dir = self.dest_dir+'/temp/'

		for files in os.listdir(tmp_dir):
			os.remove(os.path.join(tmp_dir,files))
		
		comm_loc = self.dest_dir + 'comments'
		comm_f = pd.read_json(comm_loc, lines = True, chunksize = chsize)
		
		for comm_df in comm_f:
			rjson = comm_df.to_json(orient = 'records')
			r = json.loads(rjson)
			for elem in r:
				st = elem['parent_id'][3:6]
				f = open(tmp_dir+st, 'a+',encoding = 'utf-8')
				json.dump(elem, f)
				f.write('\n')
				f.close()
		
