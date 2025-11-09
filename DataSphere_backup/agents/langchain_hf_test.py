from langchain_community.llms import HuggingFacePipeline
from transformers import pipeline

# Example: Use a free HuggingFace model (distilgpt2 for text generation)
def test_langchain_hf_agent():
    hf_pipeline = pipeline("text-generation", model="distilgpt2")
    llm = HuggingFacePipeline(pipeline=hf_pipeline)
    prompt = "What is the capital of France?"
    response = llm.invoke(prompt)
    print("LangChain agent (HuggingFace) response:", response)

if __name__ == "__main__":
    test_langchain_hf_agent()
