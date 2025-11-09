from langchain.llms import OpenAI

# Example: Simple LangChain agent that generates a response using OpenAI

def test_langchain_agent():
    # NOTE: You must set the OPENAI_API_KEY environment variable for this to work
    llm = OpenAI(temperature=0)
    prompt = "What is the capital of France?"
    response = llm(prompt)
    print("LangChain agent response:", response)

if __name__ == "__main__":
    test_langchain_agent()
