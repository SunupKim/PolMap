DEBUG_LLM = True

def generate_issue_label_gemini(
    titles: list[str],
    gen_client,
    model,
    config,
    prompt_path: str,
) -> str:
    """Gemini API를 사용하여 이슈 라벨 생성"""
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
    except Exception as e:
        print(f"GEMINI API 오류: {e}")
        return ""


def generate_issue_label_openai(
    titles: list[str],
    openai_client,
    model: str,
    prompt_path: str,
    temperature: float = 0.2,
) -> str:
    """OpenAI API를 사용하여 이슈 라벨 생성"""
    if not titles:
        return ""

    with open(prompt_path, "r", encoding="utf-8") as f:
        prompt_template = f.read()

    titles_block = "\n".join(f"- {t}" for t in titles)    
    prompt = prompt_template.replace("{titles}", titles_block)
    
    try:
        response = openai_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that creates concise issue labels."},
                {"role": "user", "content": prompt}
            ],
            temperature=temperature,
            max_tokens=50,
        )
        
        label = response.choices[0].message.content.strip()
        
        if len(label) < 3 or len(label) > 30:
            print(f"OPENAI issue label 생성 실패: 부적절한 길이 ({len(label)}자)")
            return ""
        
        if DEBUG_LLM:
            print(f"OPENAI issue label 생성 성공: {label}\n")
        
        return label
    except Exception as e:
        print(f"OPENAI API 오류: {e}")
        return ""


# 통합 함수 (기존 함수명 유지)
def generate_issue_label(
    titles: list[str],
    provider: str,
    prompt_path: str,
    **kwargs
) -> str:
    """
    LLM 제공자에 따라 적절한 함수 호출
    
    Args:
        titles: 이슈 제목 리스트
        provider: "GEMINI" 또는 "OPENAI"
        prompt_path: 프롬프트 파일 경로
        **kwargs: 각 제공자별 추가 파라미터
    """
    if provider == "GEMINI":
        return generate_issue_label_gemini(
            titles=titles,
            gen_client=kwargs.get("gen_client"),
            model=kwargs.get("model"),
            config=kwargs.get("config"),
            prompt_path=prompt_path,
        )
    elif provider == "OPENAI":
        return generate_issue_label_openai(
            titles=titles,
            openai_client=kwargs.get("openai_client"),
            model=kwargs.get("model"),
            prompt_path=prompt_path,
            temperature=kwargs.get("temperature", 0.2),
        )
    else:
        raise ValueError(f"지원하지 않는 LLM 제공자: {provider}")