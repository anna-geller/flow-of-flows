conda create --name flow-of-flows python=3.8
conda activate flow-of-flows
brew install postgresql
pip install -r requirements.txt
docker run -d --name demo_postgres -v dbdata:/var/lib/postgresql/data -p 5432:5432 -e POSTGRES_PASSWORD=your_password_1234 postgres:11
prefect create project jaffle_shop
prefect register --project jaffle_shop -p flows/
prefect agent local start
