# Hanhan_ReviewLensAI - Experiment Env


## Setup Guidance
* Download the example data, and save them in folder <b>data_input/</b>
  * Download Amazon reviews from [this link >>][3]
  * Download Google reviews from: [this link >>][4]

#### Dev Env Setup
* Run `sh setup.sh` to install required libraries.
* On your computer, install Node.js (latest LTS).
  * Run `node -v` and `npm -v` to see whether both have output. If they don't exist, ask Github Copilot how to get them installed.
* In your Virtual Env, run `npm install -g expo-cli` to install the Expo CLI, a tool that helps developers create, develop, and manage React Native projects using the Expo framework.
* Create frontend folder by running`npx create-expo-app review_lens_frontend --template blank`
* `cd review_lens_frontend/`
  * `npx expo install react-dom react-native-web` to install modules for web development
* Connect with Nero (external serverless psql)
  * I chose `AWS US West 2` --> Later Railway region should also be "US West", otherwise higher latency and potentially more costs
  * `npx neonctl@latest init`

#### Test Local Website
* Open terminal 1 and type `uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000` to start FastAPI server
  * <b>Make sure the alignment of backend folder name here</b>
  * Backend console will show printed results
* Open terminal 2, type `cd review_lens_frontend`
  * `npx expo start --web` will start the web 🚀

#### Railway Setup (backend)
* Make sure there's a `requirements.txt` in your repo
* Add `Variables`, in this case:
  * Add `GROQ_TOKEN`
  * Add `NERO_DB_URL`
* Create `start.sh` and use default Railpack for build
  * <b>make sure the alignment of the backend folder name here</b>
  * Click your Service --> Click `Settings`
  * Scroll down to `Start Command` --> enter `bash start.sh`
* After a successful deployment, click the `Service` --> Click `Settings`
* Under `Networking` --> Click `Generate Domain` --> port number can be 8080
  * You can test https://{Railway URL}/docs from browser, if it shows FastAPI page, then you're good
Doesn't need to run your local code
  * If you have multiple Railway projects, their domain can all be 8080, as long as they're separated deployments, cuz in Railway each project has its own container
  * Copy the generated domain to App.js as the value of BACKEND_URL, make sure you have `https://` before the URL!
* In your terminal, type `npm install @expo/ngrok` to allow real device access to your local frontend
* Under folder `review_lens_frontend/`, type `npx expo start --web -c`

#### Cloudflare Setup (frontend)
* Under folder "review_lens_frontend/"
  * Run `npx expo export --platform web` to export web build, this should create a "dist" folder under this frontend folder
  * In Cloudflare, search for `Worker & Pages` --> `Create New Application` --> Connect to github repo:
    * Root directory: `review_lens_frontend`
    * Framework preset: leave blank
    * Build command: `npx expo export --platform web`
    * Build output directory: `dist`
    * Add variable `EXPO_PUBLIC_BACKEND_URL` and its value, same as what's saved in `review_lens_frontend/.env`
    * "https://hanhan-reviewlensai.pages.dev" got generated
  * `Workers & Pages` --> `hanhan-reviewlensai` --> `Custom domain` --> type `reviewlens.hanhanwu.com` and wait till it's active


## References
#### Data Input
* [Amazon data][1]
* [Google data][2]


[1]:https://www.kaggle.com/datasets/kritanjalijain/amazon-reviews
[2]:https://www.kaggle.com/datasets/denizbilginn/google-maps-restaurant-reviews?select=reviews.csv
[3]:https://drive.google.com/file/d/1bVoSxy3ralaVxYOwgAAl0yPG2xs7IhMQ/view?usp=sharing
[4]:https://drive.google.com/file/d/1_n1_xBZuIrKNXfoiFK97xZQ_9xsf9WxN/view?usp=sharing