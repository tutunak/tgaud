from tgaud import sqlscript


def test_a_bare_cr_ends_a_line_comment_like_postgresql():
    items = sqlscript.parse("SELECT 1; -- note\rSELECT 2; -- crlf\r\nSELECT 3;")
    assert [i.code for i in items] == ["SELECT 1", "SELECT 2", "SELECT 3"]
    assert sqlscript.strip_comments("SELECT 1 -- c\rFROM t") == "SELECT 1 FROM t"
    # a CR-ended comment hides nothing: the statement after it is seen and checked
    assert sqlscript.parse("CREATE TABLE a (); -- x\rROLLBACK AND CHAIN;")[1].head == "ROLLBACK AND"
