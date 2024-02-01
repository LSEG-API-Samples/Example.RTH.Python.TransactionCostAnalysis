from tca_analysis.streamlit import StreamLitVisualisation

if __name__ == "__main__":
    file_path = '../data/tick_data'
    StreamLitVisualisation(file_path).perform_analysis()