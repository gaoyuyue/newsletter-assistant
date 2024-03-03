import json
import os

import requests
import yaml
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from langchain.chains import StuffDocumentsChain, LLMChain, AnalyzeDocumentChain
from langchain_community.chat_models import ChatOllama
from langchain_core.prompts import PromptTemplate

from newsletter_assistant.connection_manager import ConnectionManager
from newsletter_assistant.rss import RSSFeedLoader

load_dotenv(".env")
ollama_base_url = os.getenv("OLLAMA_BASE_URL")
wecom_hook_url = os.getenv("WECOM_HOOK_URL")

with open("config.yaml") as f:
    config = yaml.safe_load(f)


def init_db():
    with ConnectionManager(database="newsletter.db") as manager:
        manager.cursor.execute("""
    CREATE TABLE IF NOT EXISTS newsletters (
        link TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        summary TEXT NOT NULL
    )
""")
        manager.connection.commit()


init_db()


def main():
    llm = ChatOllama(
        temperature=0,
        base_url=ollama_base_url,
        model="llama2",
        top_k=10,
        top_p=0.3,
        num_ctx=3072,
    )

    document_prompt = PromptTemplate(
        input_variables=["page_content"],
        template="{page_content}"
    )
    document_variable_name = "text"
    prompt_template = """Write a concise summary of the following:


"{text}"


CONCISE SUMMARY:"""
    summary_prompt = PromptTemplate.from_template(prompt_template)
    llm_chain = LLMChain(llm=llm, prompt=summary_prompt)
    stuff_chain = StuffDocumentsChain(
        llm_chain=llm_chain,
        document_prompt=document_prompt,
        document_variable_name=document_variable_name
    )
    summarize_document_chain = AnalyzeDocumentChain(combine_docs_chain=stuff_chain)

    chain = (summarize_document_chain | (lambda resp: resp["output_text"]))

    for feed in config["feeds"]:
        loader = RSSFeedLoader(urls=[feed["url"]], text_mode=False)
        docs = loader.load()
        links = [doc.metadata["link"] for doc in docs]
        join_str = ",".join(["?" for _ in range(len(links))])
        with ConnectionManager(database="newsletter.db") as manager:
            manager.cursor.execute(f"""
            SELECT link FROM newsletters where link in ({join_str})
            """, links)
            existed_feeds = [r[0] for r in manager.cursor.fetchall()]
        new_docs = [doc for doc in docs if doc.metadata["link"] not in existed_feeds]
        for doc in new_docs:
            soup = BeautifulSoup(doc.page_content, 'html.parser')
            elements = soup.select(feed["content_selector"])
            text = ''.join([element.get_text('\n') for element in elements])
            result = chain.invoke({"input_document": text})
            newsletter = {
                "link": doc.metadata["link"],
                "title": doc.metadata["title"],
                "summary": result.strip("\n")
            }

            data = {
                "msgtype": "markdown",
                "markdown": {
                    "content": f"""
**{newsletter["title"]}**

{newsletter["summary"]}

[阅读全文]({newsletter["link"]})"""
                }
            }
            headers = {
                "Content-Type": "application/json"
            }
            response = requests.post(wecom_hook_url, data=json.dumps(data), headers=headers)
            print(response)
            with ConnectionManager(database="newsletter.db") as manager:
                manager.cursor.execute("""
                INSERT INTO newsletters VALUES (?, ?, ?)
                """, (newsletter["link"], newsletter["title"], newsletter["summary"]))
                manager.connection.commit()


if __name__ == '__main__':
    main()
