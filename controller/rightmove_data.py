import requests
import time
import re
import pandas as pd
import json
import selenium
import time
import os
from selenium import webdriver
from bs4 import BeautifulSoup

class rightmove_data():

	def __init__(self):

		self.url = "http://www.rightmove.co.uk/house-prices/detail.html?country=england&locationIdentifier=OUTCODE%5E759&searchLocation=E8&referrer=listChangeCriteria"


	def _get_rightmove_id_and_sale(self, parsed_string):
		# return rightmove_id and sale_id from a string

		x1 = parsed_string
		mylist_prop = re.split("prop=", x1)
		mylist_prop = mylist_prop[1]
		mylist_and = re.split("&sale=", mylist_prop)
		mylist_and2 = re.split("&",mylist_and[1])
		rightmove_id = int(mylist_and[0])
		sale = int(mylist_and2[0])

		return rightmove_id, sale

	def _unix2timestamp(self, unix):
		# convert from unix timstamp to human readable time
		
		xtime = time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(int(unix)))
		return xtime

	def _print_info_in_forloop_every_n_batch(self, i, batch_size, info):
		# print out a message within a for loop if the loop is at a given multiple
		
		if (i/batch_size).is_integer():
			print(info)

	def _get_df_rightmove_sales(self, html):
		# return a dataframe of rightmove_id and sale (id) for a given html page from rightmove

		# query the website and return the html to the variable ‘page’
		page = requests.get(html)
		
		if page.status_code == 200:
			# Create a BeautifulSoup object
			soup = BeautifulSoup(page.text, 'html.parser')

			# Pull all text from the SoldDetails div
			sold_details_list = soup.find_all('div', {'class': 'soldDetails'})

			n_properties = len(sold_details_list)

			rightmove_id = []
			sale = []

			for i in range(0, n_properties):

				# get a single property
				single_prop = sold_details_list[i].find_all('a')
				if len(single_prop) > 0: # not empty

					# get first href
					prop_href = single_prop[0].get('href')

					# parse and return rightmove_id and sale
					rightmove_id0, sale0 = self._get_rightmove_id_and_sale(prop_href)

					# append to lists
					rightmove_id.append(rightmove_id0)
					sale.append(sale0)
				elif len(single_prop) == 0:
					pass

			# convert rightmove_id and sale to dataframe

			rightmove = pd.DataFrame(rightmove_id, sale)
			rightmove = pd.DataFrame(
				{'rightmove_id': rightmove_id,
				 'sales': sale
				})
			
		else:
			print('error: status code = {}'.format(page.status_code))
			rightmove = pd.DataFrame(columns=['rightmove_id', 'sales'])
		
		return rightmove


	def _get_rm_address(self, rightmove_id):
		# get rightmove API address for a given rightmove_id

		rm_address = ''.join(["http://api.rightmove.co.uk/api/propertyDetails?propertyId="
							  , str(rightmove_id)
							  , "&apiApplication=IPAD"])

		return rm_address


	def _get_url_pagecount(self, url):
		""" function to get the pagenavigation pagecount from a url """
		
		# query the website and return the html to the variable ‘page’
		page = requests.get(url)

		# Create a BeautifulSoup object
		soup = BeautifulSoup(page.text, 'html.parser')

		# get page number
		span = soup.find_all('span', {'class': 'pagenavigation pagecount'})[0].get_text()
		pagination = re.split("of ", span)
		pagecount_max = int(pagination[1])
		
		return pagecount_max

	def get_property_info_from_rightmove_api(self, rightmove_id, house_properties):
		# Function to get all important information from a property using rightmove open api
		
		# get rightmove api address
		rm_id_address = self._get_rm_address(rightmove_id)

		response = requests.get(rm_id_address)

		# get data from json 
		data = json.loads(response.text)
		
		# check that the data correctly loads - HACK! --> only looks complete when 'result' = 'Success' and not 'FAILURE'
		if data['result'] == 'SUCCESS':
		
			# convert to dataframe
			pdx = pd.DataFrame(data)

			# get list of data
			data_list = pdx['property'][house_properties].tolist()

			# output dataframe with all data
			df_op = pd.DataFrame([data_list], columns=house_properties)
			
		elif data['result'] == 'FAILURE':
			pass
		
		return df_op

	def get_house_property_data(self, df_rightmove, rightmove_id_list, house_properties, show_progress=True):
		""" create function that will return a dataframe of house properties 
			for a given list of rightmove_ids
		"""
		# number of houses
		num_houses = len(rightmove_id_list)
		
		# blank dataframe to append all house property data
		df = pd.DataFrame(columns=house_properties)
		
		for i in range(0, num_houses):
			rightmove_id = df_rightmove['rightmove_id'][i]
			
			if show_progress:
				info = ('i: {}, rightmove_id: {}'.format(i, rightmove_id))
				self._print_info_in_forloop_every_n_batch(i, batch_size=50, info=info)
				
			try:
				property_info = self.get_property_info_from_rightmove_api(rightmove_id, house_properties)
			
				# append all data
				df = df.append(property_info, ignore_index=True)
				
			except ValueError:
				print('Something went wrong in accessing rightmove api')

		# convert UNIX updateDate to timestamp format
		df['updateDate'] = (df['updateDate']/1000).apply(self._unix2timestamp)
		
		return df


	def get_df_rightmove_sales_all(self, quote_page, show_progress=True):
		
		# get number of pages in search
		num_pages = self._get_url_pagecount(quote_page)

		# create blank datafrmae
		df_rightmove = pd.DataFrame(columns=['rightmove_id', 'sales'])
		
		for i in range(0, num_pages):
			
			if i == 0:
				url = quote_page
			elif i > 0:
				url = quote_page + "&index=" + str((i+1)*25)

			# get dataframe for single page and append to master dataframe
			df_sub = self._get_df_rightmove_sales(url)
			df_rightmove = df_rightmove.append(df_sub, ignore_index=True)
			info = ('page number: {}, number of properties: {}'.format(i, len(df_sub)))
			if show_progress:
				self._print_info_in_forloop_every_n_batch(i, batch_size=5, info=info)
			
		return df_rightmove

	def get_rightmove_url_from_search_term(self, search_term):
		"""function that returns the updated url from rightmove website that is filtered for a given search code."""
		
		url = self.url

		# get the path of ChromeDriverServer
		dir = os.getcwd()
		chrome_driver_path = dir + "/chromedriver"

		# create a new Chrome session
		driver = webdriver.Chrome(chrome_driver_path)
		driver.implicitly_wait(30)

		# navigate to the application home page
		driver.get(url)

		# get the search textbox
		search_field = driver.find_element_by_name("searchLocation")

		# clear search box, enter search keyword and submit
		search_field.clear()
		search_field.send_keys(search_term)
		search_field.submit()

		# get new website url
		new_url = driver.current_url

		driver.close()

		return new_url

	def save_df_to_csv(self, df, search_term, show_info=True):
		# save dataframe to data folder
		
		save_folder = os.getcwd() + '/data/'
		save_path_full = ''.join([save_folder, 'rightmove_properties_', search_term, '.csv'])
		df.to_csv(save_path_full, index=False)
		
		if show_info:
			info=('File saved to path: {}'.format(save_path_full))
			print(info)


	def get_rightmove_data(self, search_term, property_list):
		# get a csv file of all properties specified by the search_term with columns of information specified by the poperty_list

		# get updated url from the input search term
		url = self.get_rightmove_url_from_search_term(search_term)

		# get list of all rightmove ids
		df_rightmove = self.get_df_rightmove_sales_all(url, show_progress=True)
		rightmove_id_list = df_rightmove['rightmove_id'].tolist()


		# get dataframe with all current house properties
		df_all_house_data = self.get_house_property_data(df_rightmove
		                                                      , rightmove_id_list
		                                                      , property_list
		                                                      , show_progress=True)

		# save dataframe to data folder as csv
		self.save_df_to_csv(df_all_house_data, search_term, show_info=True)


