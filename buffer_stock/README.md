добавить business description!!!!

### Run locally:
  1. ``` cd ../final build   ```
  2. ``` run streamlit app.py  ```
### Run with docker:
  1. ``` cd ../final build   ```
  2. ``` sudo docker build . ```
  3. ``` docker run -p 8501:8501 *название контейнера*     ```
  all run for docker in Dockerfile.

### All requirements in requirements.txt
main lib: https://docs.streamlit.io/library/get-started/installation

### Main logic of dataset in:
 recommend_sql.py  
data model sreenshit:
<img width="1065" alt="image" src="https://user-images.githubusercontent.com/83408191/199677206-2d8a3536-2547-4c14-bfa4-1dec49d35930.png">


### Main visualization of simple table ->  ```st.dataframe(df)```:  
in app.py, using streamlit function:
![image](https://user-images.githubusercontent.com/83408191/199668571-8beb40f1-7963-4e18-92a4-9e2a77cb0deb.png)

### Excel like visualization of complex table ->  ```aggrid_interactive_table(df)``` 
in app1.py, using streamlit-aggrid function:
  - https://pypi.org/project/streamlit-aggrid/
  - https://streamlit-aggrid.readthedocs.io/en/docs/AgGrid.html 
<img width="1091" alt="image" src="https://user-images.githubusercontent.com/83408191/199673091-40730e88-86e8-476d-b5e6-95d5f0838557.png">

 
