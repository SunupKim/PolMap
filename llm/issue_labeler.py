DEBUG_LLM = True

def generate_issue_label(
    titles: list[str],
    gen_client,
    model,
    config,
    prompt_path: str,
) -> str:
    if not titles:
        return ""

    with open(prompt_path, "r", encoding="utf-8") as f:
        prompt_template = f.read()

    titles_block = "\n".join(f"- {t}" for t in titles)    
    prompt = prompt_template.replace("{titles}", titles_block)        
    
    try:
        response = gen_client.models.generate_content(
            model=model,
            contents=prompt,
            config=config,
        )

        label = response.text.strip()
        if len(label) < 3 or len(label) > 30:
            print(f"GEMINI issue label 생성 실패: 부적절한 길이 ({len(label)}자)")
            return ""
        
        if DEBUG_LLM:
            print(f"GEMINI issue label 생성 성공: {label}\n")

        return label

    except Exception:
        return ""