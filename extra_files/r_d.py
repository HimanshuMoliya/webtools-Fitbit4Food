import sqlite3
import pandas as pd
# Create your connection.
cnx = sqlite3.connect('dynamic_database.db')
df = pd.read_sql_query("SELECT * FROM product_data", cnx)
print(df)

# import streamlit as st 
# import streamlit.components.v1 as stc

# PRODUCT_CARD = '''
# 					<div style="padding: var(--su-4);border-radius:5%; position: relative; display:block;width:100%; text-align:center">
					
# 					<div class="column" style="background-color: rgb(49, 51, 63); display:inline-block; float:left; width:40%; padding: 10px; position: relative; justify-content: center">
# 						<img style="color:white; max-width:70%;" alt = "image"  src ='https://dummyimage.com/300x300'>
# 					</div>
				
# 					<div class="column" style="background-color: rgb(49, 80, 63); float: left; width: 20%; padding: 10px; position: relative; justify-content: left; text-align: left">
# 						<h2 style = "color:white;">WEBTOOLS</h2>
# 						<h3 style = "color:white;">$ k$ </h3> 
# 						<br>
# 						<a target="_blank" href="https://dummyimage.com/300x300" style = "background-color:rgb(48, 200, 0); color:white; padding:10px; border-radius:10px"> Buy Now </a>
# 						<a target="_blank" href="This is product" style = "background-color:rgb(48, 65, 0); color:white; float:right; padding:10px; border-radius:10px"> Unlock More Info </a>
# 					</div>
					
# 					</div>
#                 '''
# col1=""
# col2 = ""
# col3=""
# col4=""
# col5= ""

# for i in range(4):
#     PRODUCT_CARD ="""<style>
#     .flex__wrapper {{
#     display: flex;

#     position: relative;
#     flex-wrap: wrap;
#     }}
#     @media screen and (max-width: 30%) {{
#     .flex__wrapper {{
#         max-width: 56%;
#         background-color:white;
#     }}
#     }}
#     [class*=col--] {{
#     box-sizing: border-box;
#     flex-basis: 0;
#         flex-grow: 1;
#         max-width: 100%;
#     }}
#     .col--m-s-12 {{
#     width: 100%;
#     }}
#     .col--t-s-6 {{
#     width: 50%;
#     }}
#     img {{
#     height: 100%;
#     width: 100%;
#     object-fit:cover;
#     }}
#     </style>
    
#     <div class="flex__wrapper">
#     <div class="col--m-s-12 col--t-s-6" style="text-align:center">
#     <img style="color:white; max-width:70%;" alt = "image"  src ='https://dummyimage.com/300x300'>
#     </div>
#     <div class="col--m-s-12 col--t-s-6">
#         <div class="row--m-s-12 col--t-s-6">
#         </div>
#         <br>
#         <div>
#         <a target="_blank" href='http://www.example.com'><button target="_blank" style = "background-color:rgb(48, 200, 0); color:white; padding:10px; border-radius:10px"> Buy Now </button></a>
#         <a target="_blank" href="This is product"> 
#         <button style = "background-color:rgb(48, 65, 0); color:white; float:right; padding:10px; border-radius:10px"> Unlock More Info </button></a>
#         </div>
        
#     </div>
#     </div>
#     """.format()

#     PRODUCT_CARD ='''<div class="row" style="max-height=250px">
#     <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css">
#     <div class="col-xs-4 col-sm-2" style="text-align:center">
#         <img style="color:white; max-width:70%;" alt = "image"  src ='https://dummyimage.com/300x300'>
#     </div>
#     <div class="col-xs-6 col-sm-3">
#         <div class="row">
#             <div class="col-xs-12" style="background-color: rgb(49, 80, 63); float: left; width: 100%; padding: 3%; position: relative; justify-content: left; text-align: left">
#                 <div class="example" style = "color:black;font-size: 20px;">WEBTOOLS WEBTOOL WEBTOOL Example DIV. WEBTOOLS WEBTOOL WEBTOOL Example DIV</div>

#                 <h3 style = "color:black;">$ k$ </h3> 
        
#             </div>
#         </div>
#         <div class="row">
#         <div class="col-xs-6 col-sm-12">
#         <a target="_blank" href='http://www.example.com'><button target="_blank" style = "background-color:rgb(48, 200, 0); color:white; padding:10px; border-radius:10px"> Buy Now </button></a></div>
#         <div class="col-xs-6 visible-xs">
#         <a target="_blank" href="This is product"> 
#         <button style = "background-color:rgb(48, 65, 0); color:white; float:right; padding:10px; border-radius:10px"> Unlock More Info </button></a>
#         </div> 
#         </div>
#     </div>

#     </div>'''  

#     PRODUCT_CARD ='''
#     <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.4.1/css/bootstrap.min.css">

#     <style>

#     .ratings i {{
#         font-size: 16px;
#         color: red
#     }}

#     .strike-text {{
#         color: red;
#         text-decoration: line-through
#     }}

#     .product-image {{
#         width: 10%
#     }}

#     .dot {{
#         height: 7px;
#         width: 7px;
#         margin-left: 6px;
#         margin-right: 6px;
#         margin-top: 3px;
#         background-color: blue;
#         border-radius: 50%;
#         display: inline-block
#     }}

#     .spec-1 {{
#         color: #938787;
#         font-size: 15px
#     }}

#     h5 {{
#         font-weight: 400
#     }}

#     .para {{
#         font-size: 16px
#     }}
#     </style>
#     <body style = "background-color: transparent;">
#     <div class="container mt-2 mb-2">
#         <div class="d-flex justify-content-center row">
#             <div class="col-md-2">
#                 <div style = "background-color: #c8ffbe;" class="row p-2 border rounded">
#                     <div class="col-md-3 mt-1"><img class="img-fluid img-responsive rounded product-image" src="https://i.imgur.com/QpjAiHq.jpg"></div>
#                     <div class="col-md-6 mt-1">
#                         <h5>Quant olap shirts</h5>
#                         <div class="d-flex flex-row">
#                             <div class="ratings mr-2"><i class="fa fa-star"></i><i class="fa fa-star"></i><i class="fa fa-star"></i><i class="fa fa-star"></i></div><span>310</span>
#                         </div>
#                         <div class="mt-1 mb-1 spec-1"><span>100% cotton</span><span class="dot"></span><span>Light weight</span><span class="dot"></span><span>Best finish<br></span></div>
#                         <div class="mt-1 mb-1 spec-1"><span>Unique design</span><span class="dot"></span><span>For men</span><span class="dot"></span><span>Casual<br></span></div>
#                         <p class="text-justify text-truncate para mb-0">There are many variations of passages of Lorem Ipsum available, but the majority have suffered alteration in some form, by injected humour, or randomised words which don't look even slightly believable.<br><br></p>
#                     </div>
#                     <div class="align-items-center align-content-center col-md-3 border-left mt-1">
#                         <div class="d-flex flex-row align-items-center">
#                             <h4 class="mr-1">$13.99</h4><span class="strike-text">$20.99</span>
#                         </div>
#                         <h6 class="text-success">Free shipping</h6>
#                         <div class="d-flex flex-column mt-4">
#                         <button onClick="javascript:window.open('http://www.facebook.com', '_blank');" style = "background-color: #2e6f22; border-color: #2e6f22" class="btn btn-primary btn-sm" type="button"> <a style = "color: rgb(255, 255, 255);"> Details </a></button>

#                         <button onClick="javascript:window.open('http://www.facebook.com', '_blank');" style = "color: #2e6f22; border-color: #2e6f22;" class="btn btn-outline-primary btn-sm mt-2" type="button"><a style = "color: #2e6f22;"> Add to cart</a></button></div>
#                     </div>
#                 </div>
#             </div>
#         </div>
#     </div>    
#     </body>
#     '''
#     stc.html(PRODUCT_CARD,height=660)



#     # PRODUCT_CARD = '''
#     # 			<style>
#     # 			.flex__wrapper {{
#     # 			display: flex;
#     # 			position: relative;
#     # 			flex-wrap: wrap;
#     # 			}}
#     # 			@media screen and (min-width: 1024px) {{
#     # 			.flex__wrapper {{
#     # 				max-width: 56%;
#     # 			}}
#     # 			}}
#     # 			[class*=col--] {{
#     # 			box-sizing: border-box;
#     # 			flex-basis: 0;
#     # 				flex-grow: 1;
#     # 				max-width: 100%;
#     # 			}}

#     # 			.col--m-s-12 {{
#     # 			width: 100%;
#     # 			}}

#     # 			.col--t-s-6 {{
#     # 			width: 50%;
#     # 			}}

#     # 			img {{
#     # 			height: 100%;
#     # 			width: 100%;
#     # 			object-fit:cover;
#     # 			}}
#     # 			</style>

#     # 			<div class="flex__wrapper">
#     # 			<div class="col--m-s-12 col--t-s-6" style="text-align:center">
#     # 			<img style="color:white; max-width:70%;" alt = "image"  src ="{{img_link}}">
#     # 			</div>
#     # 			<div class="col--m-s-12 col--t-s-6">
#     # 				<h2 style = "color:white;">{{title}}</h2>
#     # 				<h3 style = "color:white;">$ {{price}} </h3> 
#     # 				<br>
#     # 				<div>
#     # 				<a target="_blank" href="{{product_link}}"><button target="_blank" style = "background-color:rgb(48, 200, 0); color:white; padding:10px; border-radius:10px"> Buy Now </button></a>
#     # 				<a target="_blank" href="{{product_detail}}"> 
#     # 				<button style = "background-color:rgb(48, 65, 0); color:white; float:right; padding:10px; border-radius:10px"> Unlock More Info </button></a>
#     # 				</div>
#     # 			</div>
#     # 			</div>
#     # 		'''.format(product_link = col1, title = col2, img_link = col3,  price = col4, product_detail = col5)


# # PRODUCT_CARD = '''
# # 					<div style="padding: var(--su-4);border-radius:10px; position: relative; display:block;width:100%; height:192px; text-align:center">
					
# # 					<div class="column" style="background-color: rgb(49, 51, 63); display:inline-block; float:left; width:40%; height:100%; padding: 10px; position: relative; justify-content: center">
# # 						<img style="color:white; max-width:70%;" alt = "image"  src ='{img_link}'>
# # 					</div>
										
# # 					<div class="column" style="background-color: rgb(49, 80, 63); float: left; width: 54%; height:100%; padding: 10px; position: relative; justify-content: left; text-align: left">
# # 						<h2 style = "color:white;">{title}</h2>
# # 						<h3 style = "color:white;">$ {price} </h3> 
# # 						<br>
# # 						<a target="_blank" href="{product_link}" style = "background-color:rgb(48, 200, 0); color:white; padding:10px; border-radius:10px"> Buy Now </a>
# # 						<a target="_blank" href="{product_detail}" style = "background-color:rgb(48, 65, 0); color:white; float:right; padding:10px; border-radius:10px"> Unlock More Info </a>
# # 					</div>
					
# # 					</div>
# # 					'''.format(product_link = col1, title = col2, img_link = col3,  price = col4, product_detail = col5)

# # 					PRODUCT_CARD = '''
# # 								<style>
# # 								.flex__wrapper {{
# # 								display: flex;
# # 								position: relative;
# # 								flex-wrap: wrap;
# # 								}}
# # 								[class*=col--] {{
# # 								box-sizing: border-box;
# # 								flex-basis: 0;
# # 									flex-grow: 1;
# # 									max-width: 100%;
# # 								}}

# # 								.col--m-s-12 {{
# # 								width: 100%;
# # 								}}

# # 								.col--t-s-6 {{
# # 								width: 50%;
# # 								}}

# # 								img {{
# # 								height: 100%;
# # 								width: 100%;
# # 								object-fit:cover;
# # 								}}
# # 								</style>

# # 								<div class="flex__wrapper">
# # 								<div class="col--m-s-12 col--t-s-6" style="text-align:center">
# # 								<img style="color:white; max-width:70%;" alt = "image"  src ="{img_link}">
# # 								</div>
# # 								<div class="col--m-s-12 col--t-s-6">
# # 									<h2 style = "color:white;">{title}</h2>
# # 									<h3 style = "color:white;">$ {price} </h3> 
# # 									<br>
# # 									<div>
# # 									<a target="_blank" href="{product_link}"><button target="_blank" style = "background-color:rgb(48, 200, 0); color:white; padding:10px; border-radius:10px"> Buy Now </button></a>
# # 									<a target="_blank" href="{product_detail}"> 
# # 									<button style = "background-color:rgb(48, 65, 0); color:white; float:right; padding:10px; border-radius:10px"> Unlock More Info </button></a>
# # 									</div>
# # 								</div>
# # 								</div>
# # 							'''.format(product_link = col1, title = col2, img_link = col3,  price = col4, product_detail = col5)
