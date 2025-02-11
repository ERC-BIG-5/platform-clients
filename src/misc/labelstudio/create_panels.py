from pathlib import Path
from string import Template
from xml import etree

from src.misc.labelstudio.check_config_duplicate_names import check_references


def create_ne_panel():
    ne_template = '''<Panel value="$NE">

                        <Text value="Nature Element '$NE' in Text" name="$NE--ne_t_0_t"/>
                        <Choices name="$NE--ne_t_0" toName="title" showInline="true">
                            <Choice value="Yes"/>
                            <Choice value="No"/>
                        </Choices>

                        <View visibleWhen="choice-selected" whenTagName="$NE--ne_t_0" whenChoiceValue="No">
                            <Text value="Nature Element '$NE' in media" name="$NE--ne_m_0_t"/>
                            <Choices name="$NE--ne_m_0" toName="title" showInline="true">
                                <Choice value="Yes"/>
                                <Choice value="No"/>
                            </Choices>
                        </View>

                        <View visibleWhen="choice-selected" whenTagName="$NE--ne_t_0" whenChoiceValue="Yes">
                            <Text value="Media rejects NE '$NE' in Text" name="$NE--ne_m_rej_0_t"/>
                            <Choices name="$NE--ne_m_rej_0" toName="title" showInline="true">
                                <Choice value="Yes"/>
                                <Choice value="No"/>
                            </Choices>
                        </View>
                    </Panel>'''

    temp_obj = Template(ne_template)
    all_panels = []
    for ne in ["Material", "Immaterial", "Biotic", "Abiotic", "Artificial surfaces",
               "Agricultural areas", "Forest and seminatural areas", "Wetlands", "Water bodies",
               "Human", "Positive", "Negative", "Other"]:
        all_panels.append(temp_obj.substitute(NE=ne))
    print("".join(all_panels))
    # still needs modification of "Other


def create_stewardship_panels():
    se_panel = """
                    <Panel value="$SA">

                        <Text value="Human-Nature Action '$SA' in Text" name="$SA--t_0_t"/>
                        <Choices name="$SA--t_0" toName="title" showInline="true">
                            <Choice value="Yes"/>
                            <Choice value="No"/>
                        </Choices>

                        <View visibleWhen="choice-selected" whenTagName="$SA--t_0" whenChoiceValue="No">
                            <Text value="Human-Nature Action '$SA' in Media" name="$SA--m_0_t"/>
                            <Choices name="$SA--m_0" toName="title" showInline="true">
                                <Choice value="Yes"/>
                                <Choice value="No"/>
                            </Choices>
                        </View>

                        <View visibleWhen="choice-selected" whenTagName="$SA--t_0" whenChoiceValue="Yes">
                            <Text value="Media rejects SA '$SA' in Text" name="$SA--m_rej_0_t"/>
                            <Choices name="$SA--m_rej_0" toName="title" showInline="true">
                                <Choice value="Yes"/>
                                <Choice value="No"/>
                            </Choices>
                        </View>

                    </Panel>
    """

    temp_obj = Template(se_panel)
    all_panels = []
    for ne in ["Preservation", "Restoration", "Sustainable_use_of_resources", "Advocacy", "Education", "Monitoring",
               "Posing", "Jogging", "Picnic", "Other"]:
        all_panels.append(temp_obj.substitute(SA=ne))
    print("".join(all_panels))


def create_relational_values_panels():
    rv_panel = """
                     <Panel value="$RV">

                <Text value="Relational Value '$RV' in Text" name="$RVid--rv_t_0_t"/>
                <Choices name="$RVid--rv_t_0" toName="title" showInline="true">
                    <Choice value="Yes"/>
                    <Choice value="No"/>
                </Choices>

                <View visibleWhen="choice-selected" whenTagName="$RVid--rv_t_0" whenChoiceValue="No">
                    <Text value="Relational Value '$RV' in Media" name="$RVid--m_0_t"/>
                    <Choices name="$RVid--m_0" toName="title" showInline="true">
                        <Choice value="Yes"/>
                        <Choice value="No"/>
                    </Choices>
                </View>

                <View visibleWhen="choice-selected" whenTagName="$RVid--rv_t_0" whenChoiceValue="Yes">
                    <Text value="Media rejects RV '$RV' in Text" name="$RVid--m_rej_0_t"/>
                    <Choices name="$RVid--m_rej_0" toName="title" showInline="true">
                        <Choice value="Yes"/>
                        <Choice value="No"/>
                    </Choices>
                </View>

                <Text name="$RVid--0_certain_t" value="Are you certain about this Relational value type?"/>
                                    <Choices name="$RVid--0_certain" toName="title" showInline="true">
                        <Choice value="Yes"/>
                        <Choice value="No"/>
                    </Choices>
                    
                            <View visibleWhen="choice-selected" whenTagName="$RVid--0_certain" whenChoiceValue="No">
                                                <Text name="$RVid--0_uncertain_other_t"
                          value="What other Relational Values could it be confused with?"/>
                      <Choices name="$RVid--0_uncertain_other" toName="title" showInline="true" choice="multiple">
                        $ARV
                    </Choices>
                            </View>
            </Panel>
    """

    temp_obj = Template(rv_panel)
    all_panels = []
    all_rvs = [
        "Personal identity",
        "Cultural identity",
        "Social cohesion",
        "Social memory",
        "Social relations",
        "Sense of place",
        "Sense of agency",
        "Spirituality",
        "Stewardship principle",
        "Stewardship eudaimonia",
        "Literacy",
        "Livelihoods",
        "Well-being",
        "Aesthetics",
        "Reciprocity",
        "Good life",
        "Kinship"
    ]
    for rv in all_rvs:
        alt_rvs = []
        for arv in all_rvs:
            if rv != arv:
                alt_rvs.append(Template('<Choice value="$ORV"/>').substitute(ORV=arv))
        alt_rvs_str = "".join(alt_rvs)
        all_panels.append(temp_obj.substitute(RV=rv, RVid=rv.replace(" ", "_"), ARV=alt_rvs_str))
    # res = "".join(all_panels)

    import xml.etree.ElementTree as ET
    f = Path("/home/rsoleyma/projects/platforms-clients/data/labelstudio_configs/test_session_1_2025.xml")
    xml_string = f.read_text(encoding="utf-8")
    tree = ET.ElementTree(ET.fromstring(xml_string))
    root = tree.getroot()
    element = root.find(".//*[@id='rv_panel']")
    # print(element)
    element.clear()
    for p in all_panels:
        print(p)
        pe = ET.fromstring(p)
        element.append(pe)
    # print(element.text)
    # print(ET.tostring(element, encoding='unicode', method='xml').strip())
    tree = ET.ElementTree(root)
    ET.indent(root, space="    ")
    tree.write("output.xml", encoding="utf-8", xml_declaration=False)
    # print(res)

    tree = ET.parse(Path("/home/rsoleyma/projects/platforms-clients/src/misc/labelstudio/output.xml"))
    root = tree.getroot()

    check_references(root)


if __name__ == "__main__":
    # create_stewardship_panels()
    create_relational_values_panels()
