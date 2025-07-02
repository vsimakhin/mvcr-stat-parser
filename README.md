Generates charts based on the official MVCR stats (https://mv.gov.cz/clanek/cizinci-s-povolenym-pobytem.aspx) for foreigners in the Czech Republic
![image](https://github.com/user-attachments/assets/595df124-091a-4e76-bb61-074c44f72c3d)


### Usage 

You need python installed. Clone or download the repository from github and run:

```bash
pipenv install
pipenv shell
python app.py
```


### Data update

- Download new files from https://mv.gov.cz/clanek/cizinci-s-povolenym-pobytem.aspx to the `source` folder
- run `python ./parser.py`