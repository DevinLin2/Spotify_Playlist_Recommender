
import { useEffect, useState } from "react";
import Button from 'react-bootstrap/Button';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Navbar from 'react-bootstrap/Navbar';
import Nav from 'react-bootstrap/Nav';
import Container from 'react-bootstrap/Container';
import Form from 'react-bootstrap/Form';
import Head from 'next/head'

export default function Home() {

  const [userInput, setUserInput] = useState("");

  function handleSubmit() {
    console.log(userInput);
  }

  return (
    <div>
      <Head>
        <title>Spotify Playlist Recommender</title>
        <meta name="viewport" content="initial-scale=1.0, width=device-width" />
      </Head>
      <Navbar bg="dark" variant="dark">
        <Container fluid>
          <Navbar.Brand>
            Spotify Playlist Recommender
          </Navbar.Brand>
        </Container>
      </Navbar>
      <Form>
        <Container>
          <Row>
            <Col>
              <Form.Group className="mb-3" controlId="title">
                <Form.Label>Enter playlist preferences:</Form.Label>
                <Form.Control type="text" placeholder="Enter preferences..." value={userInput} onChange={(e) => setUserInput(e.target.value)} />
              </Form.Group>
            </Col>
          </Row>
          <Row>
            <Col>
              <Button className="float-end" variant="primary" onClick={handleSubmit}>Get Recommendations</Button>
            </Col>
          </Row>
        </Container>
      </Form>
    </div>
  )
}
