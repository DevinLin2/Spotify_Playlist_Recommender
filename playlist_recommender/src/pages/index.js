import { useEffect, useState } from "react";
import { useSession, signIn, signOut } from 'next-auth/react';
import Button from 'react-bootstrap/Button';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Navbar from 'react-bootstrap/Navbar';
import Nav from 'react-bootstrap/Nav';
import Container from 'react-bootstrap/Container';
import Card from 'react-bootstrap/Card';
import Form from 'react-bootstrap/Form';
import Head from 'next/head'

export default function Home() {

  const [userInput, setUserInput] = useState("");
  const { data: session } = useSession();
  const [showResult, setShowResult] = useState(false);

  function handleSubmit(event) {
    event.preventDefault();
    console.log(userInput);
    setShowResult(true);
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
          {session && <Button onClick={() => signOut()}>Sign Out</Button>}
          {!session && <Button onClick={() => signIn()}>Sign In</Button>}
        </Container>
      </Navbar>
      <br></br>
      <Form onSubmit={handleSubmit}>
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
              <Button className="float-end" variant="primary" type="submit">Get Playlists</Button>
            </Col>
          </Row>
          {showResult && <div>
            <Row>
              <Col>
                Results:
              </Col>
            </Row>
            <Row>
              <Col>
                <Card style={{ width: '18rem' }}>
                  <Card.Img variant="top" src="https://i.scdn.co/image/ab67706c0000bebbc8dfff3cf472a74090fd14fc" />
                  <Card.Body>
                    <Card.Title>Lofi Hop Hop Radio 2023</Card.Title>
                    <Card.Text>
                      List of first few songs
                    </Card.Text>
                    <Button variant="primary" href="https://open.spotify.com/playlist/05OkqemhVmD27zXfdnyNsy">Go to Playlist</Button>
                  </Card.Body>
                </Card>
              </Col>
            </Row>
            <br></br>
            <Row>
              <Col>
                How were our results?
              </Col>
            </Row>
            <Row>
              <Col xs={6}>
                <Button variant="success">Excellent</Button>{' '}
                <Button variant="warning">Mediocre</Button>{' '}
                <Button variant="danger">Terrible</Button>{' '}
              </Col>
            </Row>
          </div>}
        </Container>
      </Form>
    </div>
  )
}
