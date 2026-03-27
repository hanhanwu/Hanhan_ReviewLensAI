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

#### Test Local Website
* Open terminal 1 and type `uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000` to start FastAPI server
  * Backend console will show printed results
* Open terminal 2, type `cd review_lens_frontend`
  * `npx expo start --web` will start the web 🚀


## References
#### Data Input
* [Amazon data][1]
* [Google data][2]


[1]:https://www.kaggle.com/datasets/kritanjalijain/amazon-reviews
[2]:https://www.kaggle.com/datasets/denizbilginn/google-maps-restaurant-reviews?select=reviews.csv
[3]:https://drive.google.com/file/d/1bVoSxy3ralaVxYOwgAAl0yPG2xs7IhMQ/view?usp=sharing
[4]:https://drive.google.com/file/d/1_n1_xBZuIrKNXfoiFK97xZQ_9xsf9WxN/view?usp=sharing